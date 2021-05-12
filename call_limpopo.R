#' 5/5/21 Andrey L. D. Augustynczik adapted the limpopo configuration file
#' and post-processing gams 8_merged_output scripts to automatize the link
#' between GLOBIOM and G4M 
#' 
#' In the first block, the code reads in and edit the limpopo submission 
#' scripts and launches GLOBIOM scenarios, post-processing and downscaling 
#' scripts in sequence. In the second block,... to be done
#' the code calls G4M. In the third block,... to be done

# Remove any objects from active environment so that below it will contain only the default config
rm(list=ls())

################################################################################
#                     DEFAULT CONFIGURATION PARAMETERS
################################################################################

# 1st block - Initial GLOBIOM run
#-------------------------------------------------------------------------------

# Limpopo scenario configuration - may include all sample_config options
wd <- "H:/Branch_trunk_EPA/" # working directory
project <- "test_EPA" # project name
scenarios <- "0" # scenarios to run
merge_gdx <- T # merge gdx output on limpopo

# Post-processing script configuration (8_merged_output)
limpopo_run <- "yes" # Run on limpopo yes/no
resolution <- "region37" # GLOBIOM resolution
date_label <- str_remove_all(format(Sys.Date(), "%d-%m-%Y"),"-") # Date of run
reporting_g4m <- "yes" #Reporting to G4M yes/no
reporting_iamc <- "yes" #Reporting to IAMC yes/no
reporting_iamc_g4m <- "no" # Reporting G4M to IAMC yes/no 
g4m_feedback_file <- str_glue("tabs_gui_FAOFRA2015CRF_CSIRO_t14_SSP2_iea_REGION37_",
                           date_label,"_final_csv") # G4M feedback file
regional_ag <- "ggi" # regional aggregation level
path_for_downscaling <- "H:/Downscaling/Model/input/" # path to save gdx for downscaling

# Downscaling configuration
wd_downscaling <- "H:/Downscaling/" # working directory for downscaling
merge_gdx_downscaling <- T # merge all gdx outputs on limpopo
gdx_output_name <- "downscaled" # prefix of downscaled gdx file
merge_regions <- F # merge gdx locally by scenario 
path_for_G4M <- "H:/Downscaling/Model/output/" # path to save gdx for G4M run
scenarios_for_downscaling <- "0" # full set or subset of scenarios defined previously
resolution_downscaling <- 37 # number of regions specified in the downscaling

#-------------------------------------------------------------------------------
# 2nd block - G4M run

#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
# 3rd block - final GLOBIOM run

#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------

################################################################################
#                        CONFIGURATION PROCESSING
################################################################################

# Collect the names and types of the default config parameters
config_names <- ls()
if (length(config_names) == 0) {stop("Default configuration is absent! Please restore the default configuration. It is required for configuration checking, also when providing a separate configuration file.")}
config_types <- lapply(lapply(config_names, get), typeof)

# Can now pollute the name space, load libraries

library(gdxrrw)
library(gdxtools)
library(tidyverse)
library(stringr)

# Presence of Config settings is obligatory in a config file other then for the settings listed here
OPTIONAL_CONFIG_SETTINGS <- c(
)

# Read config file if specified via an argument, check presence and types.
args <- commandArgs(trailingOnly=TRUE)
if (length(args) == 0) {
  warning("No config file argument supplied, using default run settings.")
} else if (length(args) == 1) {
  # Remove mandatory config defaults from the global scope
  rm(list=config_names[!(config_names %in% OPTIONAL_CONFIG_SETTINGS)])
  
  # Source the config file, should add mandatory config settings to the global scope
  source(args[1], local=TRUE, echo=FALSE)
  
  # Check that all config settings exist, this catches mandatory settings missing in the config file
  for (i in seq_along(config_names))  {
    name <- config_names[i]
    if (!exists(name)) stop(str_glue("Mandatory config setting {name} is not set in config file {args[1]}!"))
    type <- typeof(get(name))
    if (type != config_types[[i]] &&
        name != "resolution_downscaling" && # R has no stable numerical type
        type != "NULL" && # allow for configured vector being empty
        config_types[[i]] != "NULL" # allow for default vector being empty
    ) stop(str_glue("{name} set to wrong type in {args[1]}, type should be {config_types[[i]]}"))
  }
} else {
  stop("Multiple arguments provided! Expecting at most a single config file argument.")
}

################################################################################
#                           INITIAL GLOBIOM RUN
################################################################################

# Define wd
setwd(wd)

# Update sample_config file
tempString <- read_lines("./R/sample_config.R")
tempString[17] <- str_glue("EXPERIMENT = \"",project,"\"# label for your run")    
tempString[19] <- str_glue("JOBS = c(",scenarios,")")
tempString[40] <- str_glue("MERGE_GDX_OUTPUT = ",merge_gdx," # optional")

# Save file
write_lines(tempString, "./R/sample_config.R")

# Submit runs to limpopo
system("RScript R/Condor_run.R R/sample_config.R")

# Retrieve limpopo cluster number - cluster_nr.txt was created by modifying the Condor_run.R script
cluster_nr <- as.numeric(read_lines(str_glue("./Condor/",project,"/cluster_nr.txt")))

# create output path string
path_for_g4m2 <- str_replace_all(path_for_G4M,"/","%X%")

# Configure merged output file
tempString <- read_lines("./Model/8_merge_output.gms")
tempString[18] <- str_glue("$set limpopo    ",limpopo_run) 
tempString[20] <- str_glue("$set limpopo_nr ",cluster_nr) 
tempString[22] <- str_glue("$set project ",project) 
tempString[24] <- str_glue("$set region ",resolution)  
tempString[26] <- str_glue("$set lab ",date_label)  
tempString[29] <- str_glue("$set rep_g4m        ",reporting_g4m) 
tempString[31] <- str_glue("$set rep_iamc_glo   ",reporting_iamc)  
tempString[33] <- str_glue("$set rep_iamc_g4m   ",reporting_iamc_g4m)  
tempString[36] <- str_glue("$set g4mfile ",g4m_feedback_file)  
tempString[38] <- str_glue("$set regionagg ",regional_ag) 
tempString[151] <- "$include 8a_rep_g4m_tmp.gms" 

# Save file 
write_lines(tempString, "./Model/8_merge_output_tmp.gms")

# Point gdx output to downscaling folder
tempString <- read_lines("./Model/8a_rep_g4m.gms")
path_for_downscaling2 <- str_replace_all(path_for_downscaling,"/","%X%")

tempString[246] <- str_glue("execute_unload \"",path_for_downscaling2,"output_landcover_%project%_%lab%\"LANDCOVER_COMPARE_SCEN, LUC_COMPARE_SCEN0, Price_compare2,MacroScen, IEA_SCEN, BioenScen, ScenYear, REGION, COUNTRY,REGION_MAP") 
tempString[248] <- str_glue("execute_unload \"",path_for_g4m2,"output_globiom4g4mm_%project%_%lab%\" G4Mm_SupplyResidues, G4Mm_SupplyWood, G4Mm_Wood_price, G4Mm_LandRent,G4Mm_CO2PRICE, MacroScen, IEA_SCEN, BioenScen, ScenYear") 

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
setwd(wd)

#-------------------------------------------------------------------------------
# Retrieve G4M files - not needed as gdx will be saved directly to G4M folder 
# 
# # 1 - Socio-economic data
# f <- paste("./Model/output/g4m/output_globiom4g4mm_",project,"_",date_label,".gdx",sep="")
# globiom4g4mm_file <- gdx(f)
# all_outputs_glob4g4m <- all_items(globiom4g4mm_file)
# allparam_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$parameters,f)
# allsets_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$sets,f)
# 
# # 2 - Landcover data
# f <- paste("./Model/output/g4m/output_landcover_",project,"_",date_label,".gdx",sep="")
# landcover_file <- gdx(f)
# all_outputs_landcover <- all_items(landcover_file)
# allparam_landcover <- batch_extract_tib(all_outputs_landcover$parameters,f)
# allsets_landcover <- batch_extract_tib(all_outputs_landcover$sets,f)
# 
# # Save to G4M folder
# path_to_g4m <- "./Model/output/g4m/"
# g4m_filename_globiom4g4mm <- "globiom4g4mm_2"
# g4m_filename_landcover <- "landcover_2"
# 
# # Write gdx files to shared folder
# write.gdx(paste(path_to_g4m,"output_",g4m_filename_globiom4g4mm,"_",project,"_",date_label,".gdx",
#                 sep=""), params = allparam_glob4g4m, sets = allsets_glob4g4m)
# 
# write.gdx(paste(path_to_g4m,"output_",g4m_filename_landcover,"_",project,"_",date_label,".gdx",sep=""),
#           params = allparam_landcover, sets = allsets_landcover)
#-------------------------------------------------------------------------------

# Run Downscaling

# Change wd to downscaling folder
setwd(wd_downscaling)

# Load helper functions
source("./R/helper_functions.R")

# Configure downscaling script
tempString <- read_lines("./Model/1_downscaling.gms")
tempString[2] <- str_glue("$setglobal project ",project) 
tempString[3] <- str_glue("$setglobal lab     ",date_label) 
tempString[4] <- "$setLocal X %system.dirSep%"
tempString[676] <- str_glue("execute_unload 'gdx%X%",gdx_output_name, ".gdx',") 

# Save file 
write_lines(tempString,"./Model/1_downscaling_tmp.gms")

# Define list of scenarios and predict downscaling scenarios
scenario_mapping <- rep(0:max(eval(parse(text=scenarios))),each=resolution_downscaling)

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

tempString <- read_lines("./R/sample_config.R")
tempString[17] <- str_glue("EXPERIMENT = \"",project,"\"# label for your run")    
tempString[19] <- str_glue("JOBS = ",scen_string)

# Save file
write_lines(tempString,"./R/sample_config.R")

# Submit runs to limpopo
system("RScript R/Condor_run.R R/sample_config.R")

cluster_nr <- as.numeric(read_lines(str_glue("./Condor/",project,"/cluster_nr.txt")))

# Transfer gdx to G4M folder - in case files were merged on limpopo
if (merge_gdx_downscaling){
  # Read merged output
  f <- str_glue("./Model/gdx/downscaled_",project,"_",cluster_nr,"_merged.gdx")
  globiom4g4m_file <- gdx(f)
  all_outputs_glob4g4m <- all_items(globiom4g4m_file)
  allparam_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$parameters,f)
  allsets_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$sets,f)
  
  # Write to G4M folder
  write.gdx(str_glue(path_for_G4M,"downscaled_","output_",project,"_",date_label,".gdx"), 
            params = allparam_glob4g4m, sets = allsets_glob4g4m)
}

if (!merge_gdx_downscaling & merge_regions){
  for (i in 1:length(scenarios_for_downscaling)){
    scenarios_idx <- which(scenario_mapping %in% scenarios_for_downscaling[i]) - 1
    merge_gdx_down(project,str_glue(wd_downscaling,"Model/gdx"),scenarios_idx,
                   scenarios_for_downscaling[i],cluster_nr,path_for_G4M)
  } 
}

#-------------------------------------------------------------------------------
# Configure options for post-processing of  downscaling  - not needed if G4M uses the gdx files directly

 #setwd(wd_downscaling)
 # tempString <- readLines("./Model/2_comp_results.gms",warn=FALSE)
 # tempString[1] <- paste("$setglobal project ",project,sep="") 
 # tempString[2] <- paste("$setglobal lab     ",date_label,sep="") 
 # 
 # # Save file 
 # simDestination <- file("./Model/2_comp_results_tmp.gms", open="wt") #open file connection to write
 # writeLines(tempString, simDestination)
 # close(simDestination)
 # 
 # # Run post-processing of downscaling (will be omitted if G4M uses the gdx files)
 # rc <- tryCatch(
 #   system("gams 2_comp_results_tmp.gms"),
 #   error=function(e) e
 # )
 # if(!inherits(rc, "error")){
 #   setwd(wd_downscaling)
 #   stop(paste("Bad return from gams"))
 # }
#-------------------------------------------------------------------------------

# Back to original wd
setwd(wd)

################################################################################
#                                 G4M RUN
################################################################################



################################################################################
#                            FINAL GLOBIOM RUN
################################################################################




