
#' File with a collection of functions to run each section of
#' the GLOBIOM-G4M-link 



#' Call the initial GLOBIOM run. The function reads in and edits the Condor_run.R and sample_config.R
#' scripts and submits the scenario runs to limpopo. Subsequently, the 8_merged_output.gms script
#' is edited to match the current project and label, and export its outputs to the Downscaling folder
#' for further processing

run_globiom_initial <- function(wd,project,scenarios,merge_gdx, limpopo_run,resolution,date_label,
                                reporting_g4m,reporting_iamc,reporting_iamc_g4m,g4m_feedback_file,
                                regional_ag,path_for_downscaling,cd) 
{
  # current wd
  prior_wd <- getwd()
  
  # Define wd
  if (prior_wd != wd) setwd(wd)
  

  # Update sample_config file

#  tempString <- read_lines(str_glue(dirname(rstudioapi::getActiveDocumentContext()$path),"/sample_config_glob.R"))
  tempString <- read_lines(str_glue(cd,"/sample_config_glob.R"))
  tempString <- str_replace(tempString,"EXPERIMENT\\s{0,}=\\s{0,}[:print:]+",str_glue("EXPERIMENT = \"",project,"\"# label for your run"))
  tempString <- str_replace(tempString,"JOBS\\s{0,}=\\s{0,}[:print:]+",str_glue("JOBS = c(",scenarios,")"))
  tempString <- str_replace(tempString,"MERGE_GDX_OUTPUT\\s{0,}=\\s{0,}[:print:]+",str_glue("MERGE_GDX_OUTPUT = ",merge_gdx," # optional"))
  tempString <- str_replace(tempString,"scen_type\\s{0,}=\\s{0,}[:print:]+//","scen_type=feedback //")
  
   # Save file
  write_lines(tempString,"./R/sample_config_tmp.R")
  
  # Update Condor_run script
  tempString <- read_lines(str_glue(cd,"/Condor_run.R"))
  write_lines(tempString, "./R/Condor_run_tmp.R")
  
  # Submit runs to limpopo
  system("RScript R/Condor_run_tmp.R R/sample_config_tmp.R")
  
  # Retrieve limpopo cluster number - cluster_nr.txt was created by modifying the Condor_run.R script
  cluster_nr <- as.numeric(read_lines(str_glue("./Condor/",project,"/cluster_nr.txt")))
  
  # create output path string
  path_for_g4m2 <- str_replace_all(path_for_G4M,"/","%X%")
  
  # Configure merged output file
  tempString <- read_lines("./Model/8_merge_output.gms")
  tempString <- str_replace(tempString,"\\$set\\s+limpopo\\s+[:print:]+",str_glue("$set limpopo ",limpopo_run))
  tempString <- str_replace(tempString,"\\$set\\s+limpopo_nr\\s+[:print:]+",str_glue("$set limpopo_nr ",cluster_nr))
  tempString <- str_replace(tempString,"\\$set\\s+project\\s+[:print:]+",str_glue("$set project ",project))
  tempString <- str_replace(tempString,"\\$set\\s+region\\s+[:print:]+",str_glue("$set region ",resolution))
  tempString <- str_replace(tempString,"\\$set\\s+lab\\s+[:print:]+",str_glue("$set lab ",date_label))
  tempString <- str_replace(tempString,"\\$set\\s+rep_g4m\\s+[:print:]+",str_glue("$set rep_g4m ",reporting_g4m))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_glo\\s+[:print:]+",str_glue("$set rep_iamc_glo ",reporting_iamc))
  tempString <- str_replace(tempString,"\\$set\\s+rep_iamc_g4m\\s+[:print:]+",str_glue("$set rep_iamc_g4m ",reporting_iamc_g4m))
  tempString <- str_replace(tempString,"\\$set\\s+g4mfile\\s+[:print:]+",str_glue("$set g4mfile ",g4m_feedback_file))
  tempString <- str_replace(tempString,"\\$set\\s+regionagg\\s+[:print:]+",str_glue("$set regionagg ",regional_ag))
  tempString <- str_replace(tempString,"\\$include\\s+8a_rep_g4m","$include 8a_rep_g4m_tmp")
  
    # Save file 
  write_lines(tempString, "./Model/8_merge_output_tmp.gms")
  
  # Point gdx output to downscaling folder
  tempString <- read_lines("./Model/8a_rep_g4m.gms")
  path_for_downscaling2 <- str_replace_all(path_for_downscaling,"/","%X%")
  
  tempString <- str_replace(tempString,"execute_unload[:print:]+output_landcover[:print:]+",
                            str_glue("execute_unload \"",path_for_downscaling2,"output_landcover_%project%_%lab%\"LANDCOVER_COMPARE_SCEN, LUC_COMPARE_SCEN0, Price_compare2,MacroScen, IEA_SCEN, BioenScen, ScenYear, REGION, COUNTRY,REGION_MAP"))

  tempString <- str_replace(tempString,"execute_unload[:print:]+output_globiom4g4mm[:print:]+",
                            str_glue("execute_unload \"",path_for_g4m2,"output_globiom4g4mm_%project%_%lab%\" G4Mm_SupplyResidues, G4Mm_SupplyWood, G4Mm_Wood_price, G4Mm_LandRent,G4Mm_CO2PRICE, MacroScen, IEA_SCEN, BioenScen, ScenYear"))
  
   # Save file 
  write_lines(tempString, "./Model/8a_rep_g4m_tmp.gms")
  
  # Change wd to run post-processing file
  wd_model <- str_glue(wd,"Model/")
  setwd(wd_model)
  
  # Run post-processing script
  rc <- tryCatch(
    system("gams 8_merge_output_tmp.gms"),
    error=function(e) e
  )
  if(rc != 0){
    setwd(wd)
    stop("Bad return from gams")
  }
  
  # Return to previous wd
  setwd(prior_wd)
}


#' Call the downscaling script and export data to the subsequent G4M run. The function reads in 
#' and edits the Condor_run.R and sample_config.R scripts and submits the downscaling runs to limpopo. 
#' Subsequently, the dowscaled output is saved to the G4M input folder.

run_downscaling <- function(wd_downscaling,project,scenarios_for_downscaling,date_label,merge_gdx_downscaling,
                            gdx_output_name,merge_regions, path_for_G4M,resolution_downscaling,cd)
{
  # current wd
  prior_wd <- getwd()
  
  # Change wd to downscaling folder
  setwd(wd_downscaling)
  
  # Load helper functions
#  source("./R/helper_functions.R")
  
  # Configure downscaling script
  tempString <- read_lines("./Model/1_downscaling.gms")
  if (!any(str_detect(tempString,"%system.dirSep%"))) {
    tempString <- c("$setLocal X %system.dirSep%",tempString)
  }
  
  tempString <- str_replace(tempString,"\\$setglobal\\s+project\\s+[:print:]+",str_glue("$setglobal project ",project))
  tempString <- str_replace(tempString,"\\$setglobal\\s+lab\\s+[:print:]+",str_glue("$setglobal lab     ",date_label))
  tempString <- str_replace(tempString,"execute_unload[:print:]+",str_glue("execute_unload 'gdx%X%",gdx_output_name, ".gdx',"))  
  
   # Save file 
  write_lines(tempString,"./Model/1_downscaling_tmp.gms")
  
  # Define list of scenarios and predict downscaling scenarios
  scenario_mapping <- rep(0:max(eval(parse(text=scenarios_for_downscaling))),each=resolution_downscaling)
  
  #-------------------------------------------------------------------------------
  # # Configuration options - deprecated version of limpopo script 
  # gams_file <- "1_downscaling_tmp"
  # get_gdx <- "yes"
  
  # # limpopo configuration file
  # config_ssp <- rep("",4)
  # 
  # config_ssp[1] <- project # project name
  # config_ssp[2] <- scenario_nr # number of scenarios
  # config_ssp[3] <- gams_file # gams script name
  # config_ssp[4] <- "yes" # return gdx
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
  for (i in 1: length(scenarios_for_downscaling)){
    scenarios_idx <- which(scenario_mapping %in% scenarios_for_downscaling[i]) - 1
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  } 
  scen_string <- str_glue(scen_string,")")
  
  # Update sample_config file - needs revised 1_downscaling script and 
  # reorganization of Downscaling folder (All files into a Model folder and an R folder with limpopo scripts)
  
#  tempString <- read_lines(str_glue(dirname(rstudioapi::getActiveDocumentContext()$path),"/sample_config_down.R"))
  tempString <- read_lines(str_glue(cd,"/sample_config_down.R"))
  tempString <- str_replace(tempString," EXPERIMENT\\s{0,}=\\s{0,}[:print:]+",str_glue("EXPERIMENT = \"",project,"\"# label for your run"))
  tempString <- str_replace(tempString," JOBS\\s{0,}=\\s{0,}[:print:]+",str_glue("JOBS = ",scen_string))
  
  # Save file
  write_lines(tempString,"./R/sample_config_tmp.R")
  
  # Update Condor_run script
  tempString <- read_lines(str_glue(cd,"/Condor_run_R/Condor_run.R"))
  write_lines(tempString, "./R/Condor_run_tmp.R")
  
  # Submit runs to limpopo
  system("RScript R/Condor_run_tmp.R R/sample_config_tmp.R")
  
  cluster_nr <- as.numeric(read_lines(str_glue("./Condor/",project,"/cluster_nr.txt")))
  
  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (merge_gdx_downscaling){
    
    # Save merged output to G4M folder
    f <- str_glue("./Model/gdx/downscaled_",project,"_",cluster_nr,"_merged.gdx")
    source <- str_glue("./Model/gdx/downscaled_","output_",project,"_",date_label,".gdx")
    
    # Rename merged output file
    file.rename(from=f,to=source)
    
    # Copy to G4M folder
    file.copy(from=source,to=path_for_G4M,overwrite = T)
  }
  
  if (!merge_gdx_downscaling & merge_regions){
    for (i in 1:length(scenarios_for_downscaling)){
      scenarios_idx <- which(scenario_mapping %in% scenarios_for_downscaling[i]) - 1
      merge_gdx_down(project,str_glue(wd_downscaling,"Model/gdx"),scenarios_idx,
                     scenarios_for_downscaling[i],cluster_nr,path_for_G4M)
    } 
  }
  
  setwd(prior_wd)
}



