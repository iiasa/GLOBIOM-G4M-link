
#' File with a collection of functions to run each section of
#' the GLOBIOM-G4M-link



#' Perform the initial GLOBIOM scenarios run. The function submits the scenario
#' for parallel execution on Limpopo. Subsequently, the 8_merged_output.gms
#' script is edited to match the current project and label, and export its
#' outputs to the Downscaling folder for further processing

run_globiom_initial <- function(cd)
{
  # current wd
  #prior_wd <- getwd()

  # Define model wd
  WD <- str_glue(cd,"/",WD_GLOBIOM,"/")

  cluster_number_log <- file.path(TEMP_DIR, "cluster_number.log")
  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({SCENARIOS})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 13000',
    'REQUEST_CPUS = 1',
    'GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "6_scenarios.gms"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//nsim=\'%1\' //yes_output=1 //ssp=SSP2 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130"',
    'BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RETAIN_BUNDLE = FALSE',
    'RESTART_FILE_PATH = "t/a4_r1.g00"',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "a6_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "output.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = {MERGE_GDX}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'NICE_USER = FALSE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
  )
  config_path <- file.path(TEMP_DIR, "config_glob.R")
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Define wd
  setwd(WD)

  # Submit run to Limpopo and retrieve the run's Condor cluster number on completion
  rc <- system(str_glue("Rscript --vanilla {cd}/Condor_run_R/Condor_run.R {config_path}"))
  if (rc != 0) stop("GLOBIOM parallel Condor run on Limpopo failed!")
  cluster_nr <- readr::parse_number(read_file(cluster_number_log))

  # create output path string
  path_for_g4m2 <- str_replace_all(PATH_FOR_G4M,"/","%X%")

  # Configure merged output file
  tempString <- read_lines("./Model/8_merge_output.gms")
  tempString <- str_replace(tempString,"\\$set\\s+limpopo\\s+[:print:]+",str_glue("$set limpopo ",LIMPOPO_RUN))
  tempString <- str_replace(tempString,"\\$set\\s+limpopo_nr\\s+[:print:]+",str_glue("$set limpopo_nr ",cluster_nr))
  tempString <- str_replace(tempString,"\\$set\\s+project\\s+[:print:]+",str_glue("$set project {PROJECT}"))
  tempString <- str_replace(tempString,"\\$set\\s+region\\s+[:print:]+",str_glue("$set region ",RESOLUTION))
  tempString <- str_replace(tempString,"\\$set\\s+lab\\s+[:print:]+",str_glue("$set lab ",DATE_LABEL))
  tempString <- str_replace(tempString,"\\$set\\s+rep_g4m\\s+[:print:]+",str_glue("$set rep_g4m ",REPORTING_G4M))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_glo\\s+[:print:]+",str_glue("$set rep_iamc_glo ",REPORTING_IAMC))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_g4m\\s+[:print:]+",str_glue("$set rep_iamc_g4m ",REPORTING_IAMC_G4M))
  tempString <- str_replace(tempString,"\\$set\\s+g4mfile\\s+[:print:]+",str_glue("$set g4mfile ",G4M_FEEDBACK_FILE))
  tempString <- str_replace(tempString,"\\$set\\s+regionagg\\s+[:print:]+",str_glue("$set regionagg ",REGIONAL_AG))
  tempString <- str_replace(tempString,"\\$include\\s+8a_rep_g4m","$include 8a_rep_g4m_tmp")

    # Save file
  write_lines(tempString, "./Model/8_merge_output_tmp.gms")

  # Point gdx output to downscaling folder
  tempString <- read_lines("./Model/8a_rep_g4m.gms")

  # Create downscaling input folder if absent
  if (!dir.exists(file.path(str_glue(cd,"/",WD_DOWNSCALING,"/input/")))) dir.create(file.path(str_glue(cd,"/",WD_DOWNSCALING,"/input/")))

  path_for_downscaling2 <- str_replace_all(str_glue(cd,"/",WD_DOWNSCALING,"/input/"),"/","%X%")

  tempString <- str_replace(tempString,"execute_unload[:print:]+output_landcover[:print:]+",
                            str_glue("execute_unload \"",path_for_downscaling2,"output_landcover_%project%_%lab%\"LANDCOVER_COMPARE_SCEN, LUC_COMPARE_SCEN0, Price_compare2,MacroScen, IEA_SCEN, BioenScen, ScenYear, REGION, COUNTRY,REGION_MAP"))

  tempString <- str_replace(tempString,"execute_unload[:print:]+output_globiom4g4mm[:print:]+",
                            str_glue("execute_unload \"",path_for_g4m2,"output_globiom4g4mm_%project%_%lab%\" G4Mm_SupplyResidues, G4Mm_SupplyWood, G4Mm_Wood_price, G4Mm_LandRent,G4Mm_CO2PRICE, MacroScen, IEA_SCEN, BioenScen, ScenYear"))

   # Save file
  write_lines(tempString, "./Model/8a_rep_g4m_tmp.gms")

  # Change wd to run post-processing file
  wd_model <- str_glue(WD,"Model/")
  setwd(wd_model)

  # Run post-processing script
  rc <- tryCatch(
    system("gams 8_merge_output_tmp.gms"),
    error=function(e) e
  )
  if(rc != 0){
    setwd(WD)
    stop("Bad return from gams")
  }

  # Return to previous wd
  setwd(cd)
}


#' Call the downscaling script and export data to the subsequent G4M run. The
#' function reads in submits the downscaling runs to limpopo. Subsequently, the
#' dowscaled output is saved to the G4M input folder.

run_downscaling <- function(cd)
{
  # current wd
  #prior_wd <- getwd()

  # Configure downscaling script
  tempString <- read_lines(str_glue("./",WD_DOWNSCALING,"/1_downscaling.gms"))
  if (!any(str_detect(tempString,"%system.dirSep%"))) {
    tempString <- c("$setLocal X %system.dirSep%",tempString)
  }

  tempString <- str_replace(tempString,"\\$setglobal\\s+project\\s+[:print:]+",str_glue("$setglobal project {PROJECT}"))
  tempString <- str_replace(tempString,"\\$setglobal\\s+lab\\s+[:print:]+",str_glue("$setglobal lab     ",DATE_LABEL))
  tempString <- str_replace(tempString,"execute_unload[:print:]+",str_glue("execute_unload 'gdx%X%",GDX_OUTPUT_NAME, ".gdx',"))

   # Save file
  write_lines(tempString,str_glue("./",WD_DOWNSCALING,"/1_downscaling_tmp.gms"))

  # Define list of scenarios and predict downscaling scenarios
  scenario_mapping <- rep(0:max(eval(parse(text=SCENARIOS_FOR_DOWNSCALING))),each=RESOLUTION_DOWNSCALING)

  #-------------------------------------------------------------------------------
  # # Configuration options - deprecated version of limpopo script
  # gams_file <- "1_downscaling_tmp"
  # get_gdx <- "REGIONAL_AG"

  # # limpopo configuration file
  # config_ssp <- rep("",4)
  #
  # config_ssp[1] <- PROJECT # project name
  # config_ssp[2] <- scenario_nr # number of scenarios
  # config_ssp[3] <- gams_file # gams script name
  # config_ssp[4] <- "REGIONAL_AG" # return gdx
  #
  # # Write limpopo configuration file
  # write.table(config_ssp,"P:/globiom/Projects/Downscaling/_config_ssp_tmp.txt",
  #             col.names=F, row.names=F, quote = FALSE)
  #
  # #Call limpopo script
  # system("_start_down_tmp.bat _config_ssp_tmp.txt")
  #-------------------------------------------------------------------------------

  # Create string to specify which scenarios should be downscaled

  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scenarios_idx <- which(scenario_mapping %in% SCENARIOS_FOR_DOWNSCALING[i]) - 1
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  }
  scen_string <- str_glue(scen_string,")")

  cluster_number_log <- file.path(TEMP_DIR, "cluster_number.log")
  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({scen_string})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 2500',
    'REQUEST_CPUS = 1',
    '#GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "1_downscaling_tmp.gms"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//nsim=\'%1\'"',
    '#BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RETAIN_BUNDLE = FALSE',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "d1_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "downscaled.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = TRUE',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'NICE_USER = FALSE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
  )
  config_path <- file.path(TEMP_DIR, "config_down.R")
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Create gdx folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/gdx")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/gdx")))

  # Create output folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/output")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/output")))

    # Create Condor folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue(cd,"/",WD_DOWNSCALING,"/Condor")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/Condor")))
  if (!dir.exists(file.path(str_glue(cd,"/",WD_DOWNSCALING,"/t")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/t")))

  # Change wd to downscaling folder
  setwd(str_glue("./",WD_DOWNSCALING))

  # Submit run to Limpopo and retrieve the run's Condor cluster number on completion
  rc <- system(str_glue("Rscript --vanilla {cd}/Condor_run_R/Condor_run.R {config_path}"))
  if (rc != 0) stop("Downscaling parallel Condor run on Limpopo failed!")
  cluster_nr <- parse_number(read_file(cluster_number_log))

  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (MERGE_GDX_DOWNSCALING){

    # Save merged output to G4M folder
    f <- str_glue("gdx/downscaled_{PROJECT}_{cluster_nr}_merged.gdx")
    source <- str_glue("gdx/downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")

    # Rename merged output file
    file.rename(from=f,to=source)

    # Copy to G4M folder
    file.copy(from=source,to=PATH_FOR_G4M,overwrite = T)
  }

  if (!MERGE_GDX_DOWNSCALING & MERGE_REGIONS){
    for (i in 1:length(SCENARIOS_FOR_DOWNSCALING)){
      scenarios_idx <- which(scenario_mapping %in% SCENARIOS_FOR_DOWNSCALING[i]) - 1
      merge_gdx_down(str_glue(WD_DOWNSCALING,"Model/gdx"),scenarios_idx,
                     SCENARIOS_FOR_DOWNSCALING[i],cluster_nr,PATH_FOR_G4M)
    }
  }

  setwd(cd)
}





#' Call the final GLOBIOM run. The function reads, edits and executes the 8_merged_output.gms script
#' to generate reports for IAMC
run_globiom_final <- function(cd){

  # Define wd
  WD <- str_glue(cd,"/",WD_GLOBIOM,"/")
  setwd(WD)

  # Configure merged output file
  tempString <- read_lines("./Model/8_merge_output_tmp.gms")
  tempString <- str_replace(tempString,"\\$set\\s+rep_g4m\\s+[:print:]+",str_glue("$set rep_g4m ",REPORTING_G4M_FINAL))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_glo\\s+[:print:]+",str_glue("$set rep_iamc_glo ",REPORTING_IAMC_FINAL))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_g4m\\s+[:print:]+",str_glue("$set rep_iamc_g4m ",REPORTING_IAMC_G4M_FINAL))
  tempString <- str_replace(tempString,"\\$set\\s+g4mfile\\s+[:print:]+",str_glue("$set g4mfile ",G4M_FEEDBACK_FILE))
  tempString <- str_replace(tempString,"\\$include\\s+8c_rep_iamc_g4m","$include 8c_rep_iamc_g4m_tmp")

  # Save file
  write_lines(tempString, "./Model/8_merge_output_tmp.gms")

  # read in G4M output file
  g4m_output <- read.csv(str_glue(PATH_FOR_FEEDBACK,G4M_FEEDBACK_FILE) # Will be mofidied in the future to work with gdx files
                         , header=FALSE)

  # Define G4M scenarios
  scen <- unique(g4m_output$V2)
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_globiom_map <- str_split_fixed(scen,"__",3)

  # Check if scenario name must be treated as string
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if(special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  # Define column indices of Macro, Bioen and IEA scenarios
  macro_idx <- which(str_detect(scen_globiom_map[1,],"SSP"))
  iea_idx <- which(str_detect(scen_globiom_map[1,],"RCP"))
  bioen_idx <- 6 - macro_idx - iea_idx

  if(special_char) scen_globiom_map[,bioen_idx] <- unlist(lapply(scen_globiom_map[,bioen_idx],function(x) str_glue("\"",x,"\"")))

  # Create scenario mapping string
  for (i in 1:length(scen)){

    if (i==1) map_string <- str_glue(scen[i]," . ", scen_globiom_map[i,macro_idx], " . ",
                                     scen_globiom_map[i,bioen_idx], " . ",scen_globiom_map[i,iea_idx])
    map_string <- c(map_string,str_glue(scen[i]," . ", scen_globiom_map[i,macro_idx], " . ",
                                        scen_globiom_map[i,bioen_idx], " . ",scen_globiom_map[i,iea_idx]))
  }


  # Define sets for mapping
  g4m_globiom_map <- c("SETS","G4MScen2","/",scen,"/","","G4M_SCEN_MAP(G4MScen2,*,*,*)",
                  "/",map_string,"/",";")

  # Configure merged output file
  tempString <- read_file("./Model/8c_rep_iamc_g4m.gms")
  tempString <- str_replace(tempString,regex('SET[[:print:]*|[\r\n]*]*G4M_SCEN_MAP[[:print:]*|[\r\n]*]*/[\r\n\\s]+;'),
                            str_c(g4m_globiom_map,collapse="\n"))

  path_for_feedback2 <- str_replace_all(PATH_FOR_FEEDBACK,"/","%X%")

  tempString <- str_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                           str_glue("$include ",path_for_feedback2,G4M_FEEDBACK_FILE))

  # Save file
  write_lines(tempString, "./Model/8c_rep_iamc_g4m_tmp.gms")

  # Change wd to run post-processing file
  wd_model <- str_glue(WD,"Model/")
  setwd(wd_model)

  # Run post-processing script
  rc <- tryCatch(
    system("gams 8_merge_output_tmp.gms"),
    error=function(e) e
  )
  if(rc != 0){
    setwd(WD)
    stop("Bad return from gams")
  }

  # Return to previous wd
  setwd(cd)
}
