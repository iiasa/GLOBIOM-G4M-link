
#' File with a collection of functions to run each section of
#' the GLOBIOM-G4M-link 



#' Call the initial GLOBIOM run. The function reads in and edits the Condor_run.R and sample_config.R
#' scripts and submits the scenario runs to limpopo. Subsequently, the 8_merged_output.gms script
#' is edited to match the current project and label, and export its outputs to the Downscaling folder
#' for further processing

run_globiom_initial <- function(cd) 
{
  # current wd
  #prior_wd <- getwd()
  
  # Define model wd
  WD <- str_glue(cd,"/",WD_GLOBIOM,"/")

  # Update sample_config file
  tempString <- read_lines("./sample_config_glob.R")
  tempString <- str_replace(tempString,"EXPERIMENT\\s{0,}=\\s{0,}[:print:]+",str_glue("EXPERIMENT = \"",PROJECT,"\"# label for your run"))
  tempString <- str_replace(tempString,"JOBS\\s{0,}=\\s{0,}[:print:]+",str_glue("JOBS = c(",scenarios,")"))
  tempString <- str_replace(tempString,"MERGE_GDX_OUTPUT\\s{0,}=\\s{0,}[:print:]+",str_glue("MERGE_GDX_OUTPUT = ",merge_gdx," # optional"))
  tempString <- str_replace(tempString,"scen_type\\s{0,}=\\s{0,}[:print:]+//","scen_type=feedback //")
  
   # Save file
    # Create R folder in GLOBIOM directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_GLOBIOM,"/R")))) dir.create(file.path(str_glue("./",WD_GLOBIOM,"/R")))
  
  write_lines(tempString, str_glue("./",WD_GLOBIOM,"/R/sample_config_tmp.R"))
  
  # Update Condor_run script
  tempString <- read_lines("./Condor_run_R/Condor_run.R")
  
  # Add string to export cluster number
  cluster_string <- c("cluster_file <- file.path(run_dir, \"cluster_nr.txt\")",
                      "job_conn<-file(cluster_file, open=\"wt\")",
                      "writeLines(str_glue(predicted_cluster), job_conn)",
                      "close(job_conn)",
                      "rm(job_conn)")
  
  # Add to Condor_run script
  cluster_idx <- min(which(str_detect(tempString,"predicted_cluster")))
  tempString <- c(tempString[1:cluster_idx],cluster_string,tempString[(cluster_idx+1):length(tempString)])
  
  # Save file
  write_lines(tempString,str_glue("./",WD_GLOBIOM,"/R/Condor_run_tmp.R"))
  
  # Define wd
  setwd(WD)
              
  # Submit runs to limpopo
  system("RScript R/Condor_run_tmp.R R/sample_config_tmp.R")
  
  # Retrieve limpopo cluster number - cluster_nr.txt was created by modifying the Condor_run.R script
  cluster_nr <- readr::parse_number(read_lines(str_glue("./Condor/",PROJECT,"/cluster_nr.txt")))
  
  # create output path string
  path_for_g4m2 <- str_replace_all(PATH_FOR_G4M,"/","%X%")
  
  # Configure merged output file
  tempString <- read_lines("./Model/8_merge_output.gms")
  tempString <- str_replace(tempString,"\\$set\\s+limpopo\\s+[:print:]+",str_glue("$set limpopo ",LIMPOPO_RUN))
  tempString <- str_replace(tempString,"\\$set\\s+limpopo_nr\\s+[:print:]+",str_glue("$set limpopo_nr ",cluster_nr))
  tempString <- str_replace(tempString,"\\$set\\s+project\\s+[:print:]+",str_glue("$set project ",PROJECT))
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
  path_for_downscaling2 <- str_replace_all(str_glue(cd,"/",WD_DOWNSCALING,"/input"),"/","%X%")
  
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


#' Call the downscaling script and export data to the subsequent G4M run. The function reads in 
#' and edits the Condor_run.R and sample_config.R scripts and submits the downscaling runs to limpopo. 
#' Subsequently, the dowscaled output is saved to the G4M input folder.

run_downscaling <- function(cd)
{
  # current wd
  #prior_wd <- getwd()
  
  # Configure downscaling script
  tempString <- read_lines(str_glue("./",WD_DOWNSCALING,"/1_downscaling.gms"))
  if (!any(str_detect(tempString,"%system.dirSep%"))) {
    tempString <- c("$setLocal X %system.dirSep%",tempString)
  }
  
  tempString <- str_replace(tempString,"\\$setglobal\\s+project\\s+[:print:]+",str_glue("$setglobal project ",PROJECT))
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
  
  # Update sample_config file - needs revised 1_downscaling script and 
  # reorganization of Downscaling folder (All files into a Model folder and an R folder with limpopo scripts)
  
#  tempString <- read_lines(str_glue(dirname(rstudioapi::getActiveDocumentContext()$path),"/sample_config_down.R"))
  tempString <- read_lines("./sample_config_down.R")
  tempString <- str_replace(tempString," EXPERIMENT\\s{0,}=\\s{0,}[:print:]+",str_glue("EXPERIMENT = \"",PROJECT,"\"# label for your run"))
  tempString <- str_replace(tempString," JOBS\\s{0,}=\\s{0,}[:print:]+",str_glue("JOBS = ",scen_string))
  
    # Create R folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/R")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/R")))

  # Create gdx folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/gdx")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/gdx")))

  # Create input/output folder in downscaling directory if absent
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/input")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/input")))
  if (!dir.exists(file.path(str_glue("./",WD_DOWNSCALING,"/output")))) dir.create(file.path(str_glue("./",WD_DOWNSCALING,"/output")))  

  # Save file
  write_lines(tempString,str_glue("./",WD_DOWNSCALING,"/R/sample_config_tmp.R"))
  
  # Update Condor_run script
  tempString <- read_lines(str_glue("./",WD_DOWNSCALING,"/R/Condor_run.R"))
  
  # Add string to export cluster number
  cluster_string <- c("cluster_file <- file.path(run_dir, \"cluster_nr.txt\")",
                      "job_conn<-file(cluster_file, open=\"wt\")",
                      "writeLines(str_glue(predicted_cluster), job_conn)",
                      "close(job_conn)",
                      "rm(job_conn)")
  
  # Add to Condor_run script
  culster_idx <- min(which(str_detect(tempString,"predicted_cluster")))
  tempString <- c(tempString[1:culster_idx],cluster_string,tempString[(culster_idx+1):length(tempString)])
  
  # Save file
  write_lines(tempString, str_glue("./",WD_DOWNSCALING,"/R/Condor_run_tmp.R"))
  
  # Change wd to downscaling folder
  setwd(str_glue("./",WD_DOWNSCALING))
  
  # Submit runs to limpopo
  system("RScript R/Condor_run_tmp.R R/sample_config_tmp.R")
  
  cluster_nr <- readr::parse_number(read_lines(str_glue("./Condor/",PROJECT,"/cluster_nr.txt")))
  
  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (MERGE_GDX_DOWNSCALING){
    
    # Save merged output to G4M folder
    f <- str_glue("./gdx/downscaled_",PROJECT,"_",cluster_nr,"_merged.gdx")
    source <- str_glue("./gdx/downscaled_","output_",PROJECT,"_",DATE_LABEL,".gdx")
    
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
  
  setwd(prior_wd)
}
