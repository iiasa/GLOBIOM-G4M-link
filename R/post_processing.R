# Post-processing functions

#' Run initial post-processing
#'
#' Parameterize the GLOBIOM 8_merge_output.gms post-processing script to match
#' the current project configuration and export its output file for downscaling
#' and G4M.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_initial_postproc <- function(cluster_nr_globiom, mergeGDX=TRUE)
{
  save_environment("1")

  # Merge output GDX files to "output_{PROJECT}_{cluster_nr_globiom}_merged.gdx"
  if(mergeGDX==TRUE){
    merge_gdx(PROJECT,path(CD,WD_GLOBIOM,"Model","gdx"),cluster_nr_globiom,1000000)
  }else{
  if(!file.exists(paste0(path(CD,WD_GLOBIOM,"Model","gdx"),"/","output_", PROJECT, "_", cluster_nr_globiom, "_merged.gdx"))){merge_gdx(PROJECT,path(CD,WD_GLOBIOM,"Model","gdx"),cluster_nr_globiom,1000000)}
  }

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
                          '--lookup "{LookupType}"',
                          '--rep_g4m yes',
                          '--rep_iamc_glo yes',
                          '--rep_iamc_g4m no',
                          '--rep_iamc_forest no',
                          '--rep_iamc_biodiversity no',
                          '--gdxmerge no',
                          '--g4mfile "{G4M_FEEDBACK_FILE}"',
                          '--regionagg "{REGIONAL_AG}"',
                          '--region "REGION{REGION_RESOLUTION}"',
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
run_final_postproc <- function(cluster_nr_globiom, mergeGDX=TRUE) {

  # Create output file directory
  out_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"))
  out_dir_aux <- str_glue(str_replace_all(out_dir,"/","%X%"),"%X%")
  if(!dir_exists(out_dir)) dir_create(out_dir)

  # Create a tmp copy of the merge output file with a tmp $include
  tempString <- read_lines(path(WD_GLOBIOM, "Model", str_glue("{GLOBIOM_POSTPROC_FILE}")))
  g4m_postproc_file <- string_replace(tempString[which(str_detect(tempString,"8c_[:print:]+.gms"))],c("\\$include\\s+"),"") %>% string_replace(".gms","")
  globiom_postproc_file <- string_replace(GLOBIOM_POSTPROC_FILE,".gms","")

  if (!any(str_detect(tempString,str_glue("{g4m_postproc_file}_tmp.gms")))) tempString <- string_replace(tempString,str_glue("\\$include\\s+{g4m_postproc_file}.gms"),str_glue("$include {g4m_postproc_file}_tmp.gms"))
  # out_line <- str_glue("execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%.gdx' Output4_SSP, Output4_SSP_AG, REGION_AG_MAP")
  # tempString[which(str_detect(tempString,"execute_unload[:print:]+.gdx"))] <- out_line

  # if (any(str_detect(tempString,str_glue("execute_unload")))) tempString <- string_replace(tempString,str_glue("execute_unload[:print:]+.gdx"),str_glue("execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%.gdx"))
  if (any(str_detect(tempString,str_glue("execute_unload")))){
  out_line <- str_glue("execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%.gdx' Output4_SSP,Output4_SSP_AG,Output4_SSP_AG_2DIM,REGION_AG_MAP,REGION_MAP,Obj_Compare;execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%_all.gdx';") # if want to unload "_all" as well
  # out_line <- str_glue("execute_unload '{out_dir_aux}Output4_IAMC_template_%project%_%lab%.gdx' Output4_SSP,Output4_SSP_AG,Output4_SSP_AG_2DIM,REGION_AG_MAP,Obj_Compare;")
  tempString[which(str_detect(tempString,"execute_unload[:print:]+.gdx"))] <- out_line
  }
  write_lines(tempString, path(WD_GLOBIOM, "Model", str_glue("{globiom_postproc_file}_tmp.gms")))

  # Construct path for feedback file
  path_feedback <- str_glue(".%X%output%X%g4m%X%{PROJECT}_{DATE_LABEL}%X%")

  # Get G4M scenario list
  scen_map <-  get_mapping() %>% dplyr::select(-ScenNr) %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>%
    dplyr::select(-RegionName) %>% unique() %>% droplevels()

  length_scen1 <- scen_map[[1]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))
  length_scen2 <- scen_map[[3]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))
  length_scen3 <- scen_map[[2]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))

  # Define G4M scenarios
  scen <- matrix(unlist(str_split(get_g4m_jobs(baseline = FALSE)[-1]," ")),ncol=4,byrow=T)[,3]
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_globiom_map <- array(dim=c(length(scen),3),data="")

  for(i in 1:length(scen)){
    scen_aux <- scen[i] %>% str_split("_") %>% unlist()
    scen_globiom_map[i,1] <- do.call(str_glue, c(as.list(scen_aux[1:length_scen1[i]]), .sep = "_"))
    scen_globiom_map[i,2] <- do.call(str_glue, c(as.list(scen_aux[(length_scen1[i] + 1):(length_scen1[i] + length_scen2[i])]), .sep = "_"))
    scen_globiom_map[i,3] <- do.call(str_glue, c(as.list(scen_aux[c(-1:-(length_scen1[i] + length_scen2[i]))]), .sep = "_"))
  }


  # Check if scenario name must be treated as string
  special_char <- FALSE
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if (special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  if(special_char) scen_globiom_map[,bioen_idx] <- unlist(lapply(scen_globiom_map[,bioen_idx],function(x) str_glue("\"",x,"\"")))

  # Create scenario mapping string
  for (i in 1:length(scen)) {
    if (i == 1) {
      map_string <-
        str_glue(scen[i],
                 " . ",
                 scen_globiom_map[i, 1],
                 " . ",
                 scen_globiom_map[i, 3],
                 " . ",
                 scen_globiom_map[i, 2])
    } else {
      map_string <-
        c(
          map_string,
          str_glue(
            scen[i],
            " . ",
            scen_globiom_map[i, 1],
            " . ",
            scen_globiom_map[i, 3],
            " . ",
            scen_globiom_map[i, 2]
          )
        )
    }
  }

  # Define sets for mapping
  g4m_globiom_map <- c("G4MScen2","/",scen,"/","","G4M_SCEN_MAP(G4MScen2,*,*,*)",
                  "/",map_string,"/",";")

  # Edit mapping set
  tempString <- read_file(path(WD_GLOBIOM, "Model", str_glue("{g4m_postproc_file}.gms")))
  ## the below line is modified to accommodate for different possible separator (spaces/tabs, from GAMSIDE or GAMS Studio)
  tempString <- string_replace(tempString,regex("G4MScen2(?:.|\\s)*?G4M_SCEN_MAP(?:.|\\s)*?/\\s*;",ignore_case = T),
                               str_c(g4m_globiom_map,collapse="\n"))

  # Edit feedback file
  path_for_feedback_file <-str_glue(path_feedback, G4M_FEEDBACK_FILE)

  tempString <- string_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                           str_glue('$include "{path_for_feedback_file}"'))

  # Edit baseline scenario for forest management GHG accounting
    ref_sum_reg <-  str_glue("- G4M_SCENOUTPUT_DATA(REGION,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ScenYear)")
    ref_reg <-  "-\\s*G4M_SCENOUTPUT_DATA[:print:]REGION,[:print:]+,\"em_fm_bm[:print:]+\",ScenYear[:print:]"

    ref_sum_ctry <-  str_glue("- G4M_SCENOUTPUT_DATA_RAW(COUNTRY,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ALLYEAR)")
    ref_ctry <-  "-\\s*G4M_SCENOUTPUT_DATA_RAW[:print:]COUNTRY,[:print:]+,\"em_fm_bm[:print:]+\",ALLYEAR[:print:]"

  # Check if aggregation is done at country or region level
    country_aggregation <- str_detect(tempString,regex(ref_ctry, ignore_case = T))

    tempString <- string_replace_all(tempString,regex(ifelse(country_aggregation,ref_ctry,ref_reg), ignore_case = T),
                                ifelse(country_aggregation,ref_sum_ctry,ref_sum_reg))

    if(mergeGDX){
      gdxmerge <- "yes"
    }else{
      gdxmerge <- "no"
    }

    if(exists("cluster_nr_biodiversity")){
      process_and_report_biodiversity <- "yes"
    }else{
      process_and_report_biodiversity <- "no"
    }

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
                          '--gdxmerge "{gdxmerge}"',
                          '--lookup "{LookupType}"',
                          '--rep_g4m no',
                          '--rep_iamc_glo yes',
                          '--rep_iamc_g4m yes',
                          '--rep_iamc_forest yes',
                          '--rep_iamc_biodiversity {process_and_report_biodiversity}',
                          '--g4mfile "{G4M_FEEDBACK_FILE}"',
                          '--regionagg "{REGIONAL_AG}"',
                          '--region "REGION{REGION_RESOLUTION}"',
                          .sep = ' '))
    if (rc != 0) {
      stop(str_glue("GAMS failed with return code {rc}! See https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html#INDEX_return_21_codes_2d__21_error_21_codes"))
    }

  },
  finally = {
    setwd(prior_wd)
  })


}



#' Run initial post-processing
#'
#' Parameterize the GLOBIOM 8_merge_output.gms post-processing script to match
#' the current project configuration and export its output file for downscaling
#' and G4M.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_initial_postproc_iso <- function(cluster_nr_globiom)
{
  # Merge output GDXes to "output_{PROJECT}_{cluster_nr_globiom}_merged.gdx"
  merge_gdx(PROJECT,path(CD,WD_GLOBIOM,"Model","gdx"),cluster_nr_globiom,1000000)

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
    setwd(WD_POSTPROC)
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
run_final_postproc_iso <- function(cluster_nr_globiom){

  # Create output file directory
  out_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"))
  out_dir_aux <- str_glue(str_replace_all(out_dir,"/","%X%"),"%X%")
  if(!dir_exists(out_dir)) dir_create(out_dir)
  out_file <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"),str_glue("Output4_IAMC_template_{PROJECT}_{DATE_LABEL}.gdx"))

  # Construct path for feedback file
  path_feedback <- path(CD,WD_GLOBIOM,"Model","output","g4m",str_glue("{PROJECT}_{DATE_LABEL}"),str_glue("{G4M_FEEDBACK_FILE}"))

  # Define downscaling scenarios for limpopo run
  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_G4M)){
    if (i==1) scen_string <- str_glue(scen_string,SCENARIOS_FOR_G4M[i])
    scen_string <- str_glue(scen_string,",",SCENARIOS_FOR_G4M[i])
  }
  scen_string <- str_glue(scen_string,")")

  # Save edits and run post-processing script in the GLOBIOM Model directory
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(path(WD_POSTPROC))

    rc <- system(str_glue('gams',
                          '{GLOBIOM_POSTPROC_FILE}',
                          '--limpopo yes',
                          '--limpopo_nr {cluster_nr_globiom}',
                          '--project "{PROJECT}"',
                          '--lab "{DATE_LABEL}"',
                          '--rep_g4m no',
                          '--rep_iamc_glo yes',
                          '--rep_iamc_g4m yes',
                          '--g4mfile {path_feedback}',
                          '--outdir {out_file}',
                          '--CD {CD}',
                          '--WD_GLOBIOM {WD_GLOBIOM}',
                          '--g4m_scens "{scen_string}"',
                          '--BASE1 {BASE_SCEN1}',
                          '--BASE2 {BASE_SCEN2}',
                          '--BASE3 {BASE_SCEN3}',
                          '--regionagg {REGIONAL_AG}',
                          .sep = ' '))
    if (rc != 0) {
      stop(str_glue("GAMS failed with return code {rc}! See https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html#INDEX_return_21_codes_2d__21_error_21_codes"))
    }

  },
  finally = {
    setwd(prior_wd)
  })

}
