
#' File with a collection of functions to run each section of
#' the GLOBIOM-G4M-link

#' Run GLOBIOM scenarios
#'
#' Run the initial GLOBIOM scenarios by submitting the configured scenarios for
#' parallel execution on an HTCondor cluster.
#'
#' @return cluster_nr Cluster sequence number of HTCondor submission
run_GLOBIOM_scenarios <- function() {

  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({str_c(SCENARIOS, collapse=",")})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 13000',
    'REQUEST_CPUS = 1',
    'GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "{GLOBIOM_SCEN_FILE}"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//nsim=%1 //yes_output=1 //ssp=SSP2 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130"',
    'BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RESTART_FILE_PATH = "t/a4_r1.g00"',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "a6_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "output.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = {MERGE_GDX}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
  )

  config_path <- path(TEMP_DIR, "config_glob.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Submit GLOBIOM scenarios and wait for run completion
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_GLOBIOM)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}

#' Run initial downscaling
#'
#' Run the initial downscaling by submitting scenarios for parallel execution on
#' an HTCondor cluster.
#'
#' @return cluster_nr Cluster sequence number of HTCondor submission
run_initial_downscaling <- function() {

  # Check if input data is empty
  downs_input <- file_size(path(str_glue(CD,"/",WD_DOWNSCALING,"/input/output_landcover_{PROJECT}_{DATE_LABEL}.gdx")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")

  # Get globiom and downscaling scenario mapping
  scenario_mapping <- get_mapping() # Matching by string for now - should come directly from globiom in the future

  # Define scenarios for downscaling
  downscaling_scenarios <- subset(scenario_mapping, ScenLoop %in% SCENARIOS_FOR_DOWNSCALING)

  # Define downscaling scenarios for limpopo run
  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[i])]
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  }
  scen_string <- str_glue(scen_string,")")

  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({scen_string})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 2500',
    'REQUEST_CPUS = 1',
    'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//project=\\\"{PROJECT}\\\" //lab=\\\"{DATE_LABEL}\\\" //gdx_path=\\\"gdx/{GDX_OUTPUT_NAME}.gdx\\\" //nsim=%1"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "d1_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "downscaled.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = TRUE',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
  )
  config_path <- file.path(TEMP_DIR, "config_down.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Submit downscaling run and wait for run completion
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_DOWNSCALING)
    # Ensure that required directories exist
    if (!dir_exists('gdx')) dir_create("gdx")
    if (!dir_exists('output')) dir_create("output")
    if (!dir_exists('t')) dir_create("t")

    system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}

#' Run G4M
#'
#' Run G4N by submitting jobs for parallel execution on an HTCondor cluster.
#'
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
run_G4M <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  # Configure and run scenarios using Condor_run.R

  # Check if input data is empty
  downs_input <- file_size(path(str_glue(CD,"/",WD_G4M,"/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")
  glob_input <- file_size(path(str_glue(CD,"/",WD_G4M,"/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx")))
  if (glob_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")

  #g4m_jobs <- get_g4m_jobs(baseline = baseline)[-1] # EPA files for testing
  g4m_jobs <- get_g4m_jobs_new(baseline = baseline)[-1] # implementation for the new G4M interface
  if (length(g4m_jobs) == 1) g4m_jobs <- str_glue('"{g4m_jobs}"')

  config_template <- c(
    'PROJECT = "{PROJECT}"',
    'PREFIX = "g4m"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'G4M_EXE = "{G4M_EXE}"',
    'JOBS =',
    '{g4m_jobs}',
    'SEED_FILES = ""',
    'GAMS_CURDIR = ""', # optional, working directory for GAMS and its arguments relative to working directory, "" defaults to the working directory
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 3000',
    'REQUEST_CPUS = 1',
    'BUNDLE_INCLUDE = "*"',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'BASELINE_RUN = {baseline}'
  )

  config_path <- path(TEMP_DIR, "config_g4m.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_G4M)
    # Ensure that a sub directories for run logs and outputs exist
    if (!dir_exists('Condor')) dir_create("Condor")

    system(str_glue("Rscript --vanilla {CD}/{WD_G4M}/R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")
}

#' Run GLOBIOM post-processing
#'
#' Edit the post-processing script 8_merge_output, to match the current project
#' and label, run the script, and export its outputs to the Downscaling folder
#' for further processing
#'
#' @param cluster_nr Cluster sequence number of prior GLOBIOM HTCondor submission
run_GLOBIOM_postproc <- function(cluster_nr)
{
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_GLOBIOM)

    # create output path string
    path_for_g4m2 <- str_glue(str_replace_all(str_glue(CD,"/",PATH_FOR_G4M),"/","%X%"),"%X%")

    # Configure merged output file
    tempString <- read_lines("./Model/8_merge_output.gms")
    tempString <- string_replace(tempString,"\\$set\\s+limpopo\\s+[:print:]+",str_glue("$set limpopo ",LIMPOPO_RUN))
    tempString <- string_replace(tempString,"\\$set\\s+limpopo_nr\\s+[:print:]+",str_glue("$set limpopo_nr ",cluster_nr))
    tempString <- string_replace(tempString,"\\$set\\s+project\\s+[:print:]+",str_glue("$set project {PROJECT}"))
    tempString <- string_replace(tempString,"\\$set\\s+lab\\s+[:print:]+",str_glue("$set lab ",DATE_LABEL))
    tempString <- string_replace(tempString,"\\$set\\s+rep_g4m\\s+[:print:]+",str_glue("$set rep_g4m ",REPORTING_G4M))
    tempString <- string_replace(tempString,"\\$set\\s+rep_iamc_glo\\s+[:print:]+",str_glue("$set rep_iamc_glo ",REPORTING_IAMC))
    tempString <- string_replace(tempString,"\\$set\\s+rep_iamc_g4m\\s+[:print:]+",str_glue("$set rep_iamc_g4m ",REPORTING_IAMC_G4M))
    tempString <- string_replace(tempString,"\\$set\\s+g4mfile\\s+[:print:]+",str_glue("$set g4mfile ",G4M_FEEDBACK_FILE))
    tempString <- string_replace(tempString,"\\$set\\s+regionagg\\s+[:print:]+",str_glue("$set regionagg ",REGIONAL_AG))
    tempString <- string_replace(tempString,"\\$include\\s+8a_rep_g4m","$include 8a_rep_g4m_tmp")

    # Save file
    write_lines(tempString, "Model/8_merge_output_tmp.gms")

    # Point gdx output to downscaling folder
    tempString <- read_lines("Model/8a_rep_g4m.gms")

    # Create downscaling input folder if absent
    if (!dir_exists(path(CD,"/",WD_DOWNSCALING,"/input/"))) dir_create(path(CD,"/",WD_DOWNSCALING,"/input/"))

    # Create G4M output folder if absent
    if (!dir_exists(path(CD,"/",PATH_FOR_G4M))) dir_create(path(CD,"/",PATH_FOR_G4M))

    path_for_downscaling2 <- str_glue(str_replace_all(path(CD,"/",WD_DOWNSCALING,"/input/"),"/","%X%"),"%X%")

    tempString <- str_replace(tempString,"execute_unload[:print:]+output_landcover[:print:]+",
                              str_glue("execute_unload \"",path_for_downscaling2,"output_landcover_%project%_%lab%\"LANDCOVER_COMPARE_SCEN, LUC_COMPARE_SCEN0, Price_compare2,MacroScen, IEA_SCEN, BioenScen, ScenYear, REGION, COUNTRY,REGION_MAP"))

    tempString <- str_replace(tempString,"execute_unload[:print:]+output_globiom4g4mm[:print:]+",
                              str_glue("execute_unload \"",path_for_g4m2,"output_globiom4g4mm_%project%_%lab%\" G4Mm_SupplyResidues, G4Mm_SupplyWood, G4Mm_Wood_price, G4Mm_LandRent,G4Mm_CO2PRICE, MacroScen, IEA_SCEN, BioenScen, ScenYear"))

    # Save file
    write_lines(tempString, "Model/8a_rep_g4m_tmp.gms")

    # Run post-processing script in the Model directory
    setwd("Model")
    rc <- system("gams 8_merge_output_tmp.gms")
    if (rc != 0) {
      stop(str_glue("GAMS failed with return code {rc}! See https://www.gams.com/latest/docs/UG_GAMSReturnCodes.html#INDEX_return_21_codes_2d__21_error_21_codes"))
    }
  },
  finally = {
    setwd(prior_wd)
  })
}


#' Function to transfer the downscaling output to G4M folder. Merges the downscaled
#' regions if required
#'
#' #' @param cluster_nr Cluster sequence number of prior downscaling HTCondor submission
merge_and_transfer <- function(cluster_nr){
  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (MERGE_GDX_DOWNSCALING){

    # Save merged output to G4M folder
    f <- str_glue(WD_DOWNSCALING,"/gdx/downscaled_{PROJECT}_{cluster_nr}_merged.gdx")
    file_copy(f,path(str_glue(CD,"/",PATH_FOR_G4M,"/downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")),overwrite = TRUE)
  }

  if (!MERGE_GDX_DOWNSCALING & MERGE_REGIONS){

    for (i in 1:length(SCENARIOS_FOR_DOWNSCALING)){
      scenarios_idx <- which(scenario_mapping %in% SCENARIOS_FOR_DOWNSCALING[i]) - 1
      merge_gdx_down(str_glue(WD_DOWNSCALING,"/gdx"),scenarios_idx,
                     SCENARIOS_FOR_DOWNSCALING[i],cluster_nr,PATH_FOR_G4M)
    }
  }
}

#' Function to collect G4M results from binary files and write a csv file for GLOBIOM
#;
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
compile_g4m_data <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_G4M)

    # Path of output folder
    file_path <- path_wd(str_glue("/out/{PROJECT}_{DATE_LABEL}/"))

    # Suffix of scenario runs
    file_suffix <- str_glue("_{PROJECT}_{DATE_LABEL}_") # for now in future should be indexed by project and date label

    # G4M scenario ID
    g4m_jobs <- get_g4m_jobs_new(baseline = baseline)[-1]

    # Split dimensions and extract scenario name
    scenarios_split <- str_split_fixed(g4m_jobs," ",n=4)
    scenarios <- scenario_names <- scenarios_split[,3]

    # Number of scenarios
    N <- length(scenarios)

    # Extract CO2 price
    co2 <- abs(as.integer(str_replace(scenarios_split[,4],",","")))

    # Compile results
    generate_g4M_report(file_path,file_suffix,scenarios,scenario_names,N,co2)

    # Edit and save csv file for GAMS
    g4m_out <- read.csv(path(file_path,G4M_FEEDBACK_FILE))
    colnames(g4m_out)[1:3] <- ""
    colnames(g4m_out) <- str_replace_all(colnames(g4m_out),"X","")
    write.csv(g4m_out,path(file_path,G4M_FEEDBACK_FILE),row.names = F,quote = T)
  },
  finally = {
    setwd(prior_wd)
  })
}

#' Final post-processing. The function reads, edits and executes the
#' 8_merged_output.gms script #' to generate reports for IAMC
#'
run_postproc_final <- function(wd){

  # Define wd
  setwd(wd)

  # Configure merged output file
  tempString <- read_lines("./Model/8_merge_output_tmp.gms")
  tempString <- string_replace(tempString,"\\$set\\s+rep_g4m\\s+[:print:]+",str_glue("$set rep_g4m ",REPORTING_G4M_FINAL))
  tempString <- string_replace(tempString,"\\$set\\s+rep_iamc_glo\\s+[:print:]+",str_glue("$set rep_iamc_glo ",REPORTING_IAMC_FINAL))
  tempString <- string_replace(tempString,"\\$set\\s+rep_iamc_g4m\\s+[:print:]+",str_glue("$set rep_iamc_g4m ",REPORTING_IAMC_G4M_FINAL))
  tempString <- string_replace(tempString,"\\$set\\s+g4mfile\\s+[:print:]+",str_glue("$set g4mfile ",G4M_FEEDBACK_FILE))
  if (!any(str_detect(tempString,"8c_rep_iamc_g4m_tmp.gms"))) tempString <- string_replace(tempString,"\\$include\\s+8c_rep_iamc_g4m.gms","$include 8c_rep_iamc_g4m_tmp.gms")

  # Save file
  write_lines(tempString, "./Model/8_merge_output_tmp.gms")

  # Define path for feedback file
  path_feedback <- path(CD,WD_G4M,PATH_FOR_FEEDBACK)

  # read in G4M output file
  g4m_output <- read.csv(path(str_glue(path_feedback,"/",G4M_FEEDBACK_FILE)) # Will be modified in the future to work with gdx files
                         , header=FALSE)

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
  for (i in 1:length(scen)){

    if (i==1) { map_string <- str_glue(scen[i]," . ", scen_globiom_map[i,macro_idx], " . ",
                                     scen_globiom_map[i,bioen_idx], " . ",scen_globiom_map[i,iea_idx]) } else {
    map_string <- c(map_string,str_glue(scen[i]," . ", scen_globiom_map[i,macro_idx], " . ",
                                        scen_globiom_map[i,bioen_idx], " . ",scen_globiom_map[i,iea_idx]))}
  }


  # Define sets for mapping
  g4m_globiom_map <- c("G4MScen2","/",scen,"/","","G4M_SCEN_MAP(G4MScen2,*,*,*)",
                  "/",map_string,"/",";")

  # Configure merged output file
  tempString <- read_file("./Model/8c_rep_iamc_g4m.gms")
  tempString <- string_replace(tempString,regex('G4MScen2[[:print:]*|[\r\n]*]*G4M_SCEN_MAP[[:print:]*|[\r\n]*]*/[\r\n\\s]+;'),
                            str_c(g4m_globiom_map,collapse="\n"))

  path_for_feedback2 <- str_glue(str_replace_all(path_feedback,"/","%X%"),"%X%")

  tempString <- string_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                           str_glue("$include ",path_for_feedback2,G4M_FEEDBACK_FILE))

  # Save file
  write_lines(tempString, "./Model/8c_rep_iamc_g4m_tmp.gms")

  # Change wd to run post-processing file
  wd_model <- path(wd,"/Model/")
  setwd(wd_model)

  # Run post-processing script
  rc <- tryCatch(
    system("gams 8_merge_output_tmp.gms"),
    error=function(e) e
  )
  if(rc != 0){
    setwd(CD)
    stop("Bad return from gams")
  }

  # Return to previous wd
  setwd(CD)
}

