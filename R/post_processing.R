# Post-processing functions

#' Run initial post-processing
#'
#' Parameterize the GLOBIOM 8_merge_output.gms post-processing script to match
#' the current project configuration and export its output file for downscaling
#' and G4M.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_initial_postproc <- function(cluster_nr_globiom)
{
  save_environment("1")

  # Check solution for infeasibilities
  check_sol(cluster_nr_globiom)

   # Create downscaling input folder if absent
  if (!dir_exists(path(CD, WD_DOWNSCALING, "input"))) dir_create(path(CD, WD_DOWNSCALING, "input"))

  # Create G4M input folder if absent
  if (!dir_exists(path(CD, PATH_FOR_G4M))) dir_create(path(CD, PATH_FOR_G4M))

  # Construct path of landcover output file
  output_landcover <- path(CD, WD_DOWNSCALING, "input", str_glue("output_landcover_{PROJECT}_{DATE_LABEL}"))

  # Construct path of output file for G4M
  output_globiom4g4mm <- path(CD, PATH_FOR_G4M, str_glue("output_globiom4g4mm_{PROJECT}_{DATE_LABEL}"))

  # Run post-processing script in the GLOBIOM Model directory
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(path(WD_GLOBIOM, "Model"))
    rc <- system(str_glue('gams',
                          '"{GLOBIOM_POSTPROC_FILE}"',
                          '--limpopo yes',
                          '--limpopo_nr "{cluster_nr_globiom}"',
                          '--project "{PROJECT}"',
                          '--lab "{DATE_LABEL}"',
                          '--rep_g4m yes',
                          '--rep_iamc_glo yes',
                          '--rep_iamc_g4m no',
                          '--g4mfile "{G4M_FEEDBACK_FILE}"',
                          '--regionagg "{REGIONAL_AG}"',
                          '--output_landcover "{output_landcover}"',
                          '--output_globiom4g4mm "{output_globiom4g4mm}"',
                          .sep = ' '))
    if (rc != 0) {
      stop(str_glue("GAMS failed with return code {rc}! See https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html#INDEX_return_21_codes_2d__21_error_21_codes"))
    }
  },
  finally = {
    setwd(prior_wd)
  })

  # Save global environment
  save_environment("2")
}

#' Run final post-processing
#'
#' Reads, edits and executes the GLOBIOM 8_merged_output.gms script to generate
#' reports for IAMC.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_final_postproc <- function(cluster_nr_globiom) {

  # Create output file directory
  out_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"))
  out_dir_aux <- str_glue(str_replace_all(out_dir,"/","%X%"),"%X%")
  if(!dir_exists(out_dir)) dir_create(out_dir)

  # Create a tmp copy of the merge output file with a tmp $include
  tempString <- read_lines(path(WD_GLOBIOM, "Model", str_glue("{GLOBIOM_POSTPROC_FILE}")))
  g4m_postproc_file <- string_replace(tempString[which(str_detect(tempString,"8c_[:print:]+.gms"))],c("\\$include\\s+"),"") %>% string_replace(".gms","")
  globiom_postproc_file <- string_replace(GLOBIOM_POSTPROC_FILE,".gms","")

  if (!any(str_detect(tempString,str_glue("{g4m_postproc_file}_tmp.gms")))) tempString <- string_replace(tempString,str_glue("\\$include\\s+{g4m_postproc_file}.gms"),str_glue("$include {g4m_postproc_file}_tmp.gms"))
  out_line <- str_glue("execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%.gdx' Output4_SSP, Output4_SSP_AG, REGION_AG_MAP")
  tempString[which(str_detect(tempString,"execute_unload[:print:]+.gdx"))] <- out_line
  write_lines(tempString, path(WD_GLOBIOM, "Model", str_glue("{globiom_postproc_file}_tmp.gms")))

  # Construct path for feedback file
  path_feedback <- str_glue(".%X%output%X%g4m%X%{PROJECT}_{DATE_LABEL}%X%")

  # rGet G4M scenario list
  scen_map <-  droplevels(subset(unique(get_mapping()[1,-4]), ScenLoop %in% SCENARIOS_FOR_G4M))
  length_scen1 <- length(unlist(str_split(scen_map[,1],"_")))
  length_scen2 <- length(unlist(str_split(scen_map[,3],"_")))
  length_scen3 <- length(unlist(str_split(scen_map[,2],"_")))

  # Define G4M scenarios
  scen <- matrix(unlist(str_split(get_g4m_jobs(baseline = FALSE)[-1]," ")),ncol=4,byrow=T)[,3]
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_aux <- str_split(scen,"_")
  scen_aux <- matrix(unlist(scen_aux),ncol = length(scen_aux[[1]]), byrow = TRUE)
  scen_globiom_map <- array(dim=c(length(scen),3),data="")
  for(i in 1:dim(scen_aux)[1]){
    scen_globiom_map[i,1] <- do.call(str_glue, c(as.list(scen_aux[i,1:length_scen1]), .sep = "_"))
    scen_globiom_map[i,2] <- do.call(str_glue, c(as.list(scen_aux[i,(length_scen1 + 1):(length_scen1 + length_scen2)]), .sep = "_"))
    scen_globiom_map[i,3] <- do.call(str_glue, c(as.list(scen_aux[i,c(-1:-(length_scen1 + length_scen2))]), .sep = "_"))
  }


  # Check if scenario name must be treated as string
  special_char <- FALSE
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if (special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  # Define column indices of Macro, Bioen and IEA scenarios
  macro_idx <- which(str_detect(scen_globiom_map[1,],regex("SSP",ignore_case = T)))
  iea_idx <- which(str_detect(scen_globiom_map[1,],regex("RCP",ignore_case = T)))
  bioen_idx <- 6 - macro_idx - iea_idx

  if(special_char) scen_globiom_map[,bioen_idx] <- unlist(lapply(scen_globiom_map[,bioen_idx],function(x) str_glue("\"",x,"\"")))

  # Create scenario mapping string
  for (i in 1:length(scen)) {
    if (i == 1) {
      map_string <-
        str_glue(scen[i],
                 " . ",
                 scen_globiom_map[i, macro_idx],
                 " . ",
                 scen_globiom_map[i, bioen_idx],
                 " . ",
                 scen_globiom_map[i, iea_idx])
    } else {
      map_string <-
        c(
          map_string,
          str_glue(
            scen[i],
            " . ",
            scen_globiom_map[i, macro_idx],
            " . ",
            scen_globiom_map[i, bioen_idx],
            " . ",
            scen_globiom_map[i, iea_idx]
          )
        )
    }
  }

  # Define sets for mapping
  g4m_globiom_map <- c("G4MScen2","/",scen,"/","","G4M_SCEN_MAP(G4MScen2,*,*,*)",
                  "/",map_string,"/",";")

  # Edit mapping set
  tempString <- read_file(path(WD_GLOBIOM, "Model", str_glue("{g4m_postproc_file}.gms")))
  tempString <- string_replace(tempString,regex('G4MScen2[[:print:]*|[\r\n]*]*G4M_SCEN_MAP[[:print:]*|[\r\n]*]*/[\r\n\\s]+;'),
                            str_c(g4m_globiom_map,collapse="\n"))

  # Edit feedback file
  path_for_feedback_file <-str_glue(path_feedback, G4M_FEEDBACK_FILE)

  tempString <- string_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                           str_glue('$include "{path_for_feedback_file}"'))

  # Edit baseline scenario for forest management GHG accounting
    ref_sum_reg <-  str_glue("- G4M_SCENOUTPUT_DATA(REGION,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ScenYear)")
    ref_reg <-  "-\\s+G4M_SCENOUTPUT_DATA[:print:]REGION,[:print:]+,\"em_fm_bm_mtco2year\",ScenYear[:print:]"+

    ref_sum_ctry <-  str_glue("- G4M_SCENOUTPUT_DATA(COUNTRY,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ScenYear)")
    ref_ctry <-  "-\\s+G4M_SCENOUTPUT_DATA[:print:]COUNTRY,[:print:]+,\"em_fm_bm_mtco2year\",ScenYear[:print:]"

  # Check if aggregation is done at country or region level
    country_aggregation <- str_detect(tempString,regex(ref_ctry, ignore_case = T))

    tempString <- string_replace_all(tempString,regex(ifelse(country_aggregation,ref_ctry,ref_reg), ignore_case = T),
                                ifelse(country_aggregation,ref_sum_ctry,ref_sum_reg))

  # Save edits and run post-processing script in the GLOBIOM Model directory
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(path(WD_GLOBIOM, "Model"))
    write_lines(tempString, str_glue("{g4m_postproc_file}_tmp.gms"))
    rc <- system(str_glue('gams',
                          '{globiom_postproc_file}_tmp.gms',
                          '--limpopo yes',
                          '--limpopo_nr "{cluster_nr_globiom}"',
                          '--project "{PROJECT}"',
                          '--lab "{DATE_LABEL}"',
                          '--rep_g4m no',
                          '--rep_iamc_glo yes',
                          '--rep_iamc_g4m yes',
                          '--g4mfile "{G4M_FEEDBACK_FILE}"',
                          '--regionagg "{REGIONAL_AG}"',
                          .sep = ' '))
    if (rc != 0) {
      stop(str_glue("GAMS failed with return code {rc}! See https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html#INDEX_return_21_codes_2d__21_error_21_codes"))
    }

  },
  finally = {
    setwd(prior_wd)
  })


}
