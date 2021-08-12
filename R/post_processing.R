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
                          '8_merge_output.gms',
                          '--limpopo "{LIMPOPO_RUN}"',
                          '--limpopo_nr "{cluster_nr_globiom}"',
                          '--project "{PROJECT}"',
                          '--lab "{DATE_LABEL}"',
                          '--rep_g4m "{REPORTING_G4M}"',
                          '--rep_iamc_glo "{REPORTING_IAMC}"',
                          '--rep_iamc_g4m "{REPORTING_IAMC_G4M}"',
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
}

#' Run final post-processing
#'
#' Reads, edits and executes the GLOBIOM 8_merged_output.gms script to generate
#' reports for IAMC.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_final_postproc <- function(cluster_nr_globiom) {

  # Create a tmp copy of the merge output file with a tmp $include
  tempString <- read_lines(path(WD_GLOBIOM, "Model", "8_merge_output.gms"))
  if (!any(str_detect(tempString,"8c_rep_iamc_g4m_tmp.gms"))) tempString <- string_replace(tempString,"\\$include\\s+8c_rep_iamc_g4m.gms","$include 8c_rep_iamc_g4m_tmp.gms")
  write_lines(tempString, path(WD_GLOBIOM, "Model", "8_merge_output_tmp.gms"))

  # Construct path of landcover output file
  output_landcover <- path(CD, WD_DOWNSCALING, "input", str_glue("output_landcover_{PROJECT}_{DATE_LABEL}"))

  # Construct path of output file for G4M
  output_globiom4g4mm <- path(CD, PATH_FOR_G4M, str_glue("output_globiom4g4mm_{PROJECT}_{DATE_LABEL}"))

  # Construct path for feedback file
  path_feedback <- path(WD_G4M, PATH_FOR_FEEDBACK)

  # read in G4M output file
  g4m_output <- read.csv(path(path_feedback, G4M_FEEDBACK_FILE), header=FALSE) # Will be modified in the future to work with gdx files

  # Define G4M scenarios
  scen <- unique(g4m_output[,2])
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_globiom_map <- str_split_fixed(scen,"_",3)

  # Check if scenario name must be treated as string
  special_char <- FALSE
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if (special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  # Define column indices of Macro, Bioen and IEA scenarios
  macro_idx <- which(str_detect(scen_globiom_map[1,],"SSP"))
  iea_idx <- which(str_detect(scen_globiom_map[1,],"RCP"))
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

  # Edit
  tempString <- read_file(path(WD_GLOBIOM, "Model", "8c_rep_iamc_g4m.gms"))
  tempString <- string_replace(tempString,regex('G4MScen2[[:print:]*|[\r\n]*]*G4M_SCEN_MAP[[:print:]*|[\r\n]*]*/[\r\n\\s]+;'),
                            str_c(g4m_globiom_map,collapse="\n"))

  path_for_feedback_file <-path_wd(path_feedback, G4M_FEEDBACK_FILE)

  tempString <- string_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                           str_glue('$include "{path_for_feedback_file}"'))

  # Save edits and run post-processing script in the GLOBIOM Model directory
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(path(WD_GLOBIOM, "Model"))
    write_lines(tempString, "8c_rep_iamc_g4m_tmp.gms")
    rc <- system(str_glue('gams',
                          '8_merge_output_tmp.gms',
                          '--limpopo "{LIMPOPO_RUN}"',
                          '--limpopo_nr "{cluster_nr_globiom}"',
                          '--project "{PROJECT}"',
                          '--lab "{DATE_LABEL}"',
                          '--rep_g4m "{REPORTING_G4M_FINAL}"',
                          '--rep_iamc_glo "{REPORTING_IAMC_FINAL}"',
                          '--rep_iamc_g4m "{REPORTING_IAMC_G4M_FINAL}"',
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
}
