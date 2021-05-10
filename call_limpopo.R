#' 5/5/21 Andrey L. D. Augustynczik adapted the limpopo configuration file
#' and post-processing gams 8_merged_output scripts to automatize the link
#' between GLOBIOM and G4M 
#' 
#' In the first block, the code reads in and edit the limpopo submission 
#' scripts and launches GLOBIOM scenarios, post-processing and downscaling 
#' scripts in sequence. In the second block,... to be done
#' the code calls G4M. In the third block,... to be done


# Load libraries 

library(gdxrrw)
library(gdxtools)
library(tidyverse)
library(stringr)

################################################################################
#                        CONFIGURATION PARAMETERS
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
date_label <- gsub("-","",format(Sys.Date(), "%d-%m-%Y")) # Date of run
reporting_g4m <- "yes" #Reporting to G4M yes/no
reporting_iamc <- "yes" #Reporting to IAMC yes/no
reporting_iamc_g4m <- "no" # Reporting G4M to IAMC yes/no 
g4m_feedback_file <- paste0("tabs_gui_FAOFRA2015CRF_CSIRO_t14_SSP2_iea_REGION37_",
                           date_label,"_final_csv") # G4M feedback file
regional_ag <- "ggi" # regional aggregation level
path_for_downscaling <- "H:/Downscaling/Model/input/" # path to save gdx for downscaling


#Downscaling configuration
wd_downscaling <- "H:/Downscaling/" # working directory for downscaling
merge_gdx_downscaling <- T # merge all gdx outputs on limpopo
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
#                           INITIAL GLOBIOM RUN
################################################################################

# Define wd
setwd(wd)

# Update sample_config file
tempString <- readLines("./R/sample_config.R",warn=FALSE)
tempString[17] <- paste0("EXPERIMENT = \"",project,"\"# label for your run")    
tempString[19] <- paste0("JOBS = c(",scenarios,")")
tempString[40] <- paste0("MERGE_GDX_OUTPUT = ",merge_gdx," # optional")
simDestination <- file("./R/sample_config.R", open="wt") #open file connection to write
writeLines(tempString, simDestination)
close(simDestination)

# Submit runs to limpopo
system("RScript R/Condor_run.R R/sample_config.R")

# Retrieve limpopo cluster number - cluster_nr.txt was created by modifying the Condor_run.R script
cluster_nr <- as.numeric(readLines(paste0("./Condor/",project,"/cluster_nr.txt"),warn=FALSE))

# create output path string
path_for_g4m2 <- gsub("/","%X%",path_for_G4M)

# Configure merged output file
tempString <- readLines("./Model/8_merge_output.gms",warn=FALSE)
tempString[18] <- paste0("$set limpopo    ",limpopo_run) 
tempString[20] <- paste0("$set limpopo_nr ",cluster_nr) 
tempString[22] <- paste0("$set project ",project) 
tempString[24] <- paste0("$set region ",resolution)  
tempString[26] <- paste0("$set lab ",date_label)  
tempString[29] <- paste0("$set rep_g4m        ",reporting_g4m) 
tempString[31] <- paste0("$set rep_iamc_glo   ",reporting_iamc)  
tempString[33] <- paste0("$set rep_iamc_g4m   ",reporting_iamc_g4m)  
tempString[36] <- paste0("$set g4mfile ",g4m_feedback_file)  
tempString[38] <- paste0("$set regionagg ",regional_ag) 
tempString[151] <- "$include 8a_rep_g4m_tmp.gms" 

# Save file 
simDestination <- file("./Model/8_merge_output_tmp.gms", open="wt") #open file connection to write
writeLines(tempString, simDestination)
close(simDestination)

# Point gdx output to downscaling folder
tempString <- readLines("./Model/8a_rep_g4m.gms",warn=FALSE)
path_for_downscaling2 <- gsub("/","%X%",path_for_downscaling)

tempString[246] <- paste0("execute_unload \"",path_for_downscaling2,"output_landcover_%project%_%lab%\"LANDCOVER_COMPARE_SCEN, LUC_COMPARE_SCEN0, Price_compare2,MacroScen, IEA_SCEN, BioenScen, ScenYear, REGION, COUNTRY,REGION_MAP") 
tempString[248] <- paste0("execute_unload \"",path_for_g4m2,"output_globiom4g4mm_%project%_%lab%\" G4Mm_SupplyResidues, G4Mm_SupplyWood, G4Mm_Wood_price, G4Mm_LandRent,G4Mm_CO2PRICE, MacroScen, IEA_SCEN, BioenScen, ScenYear") 

# Save file 
simDestination <- file("./Model/8a_rep_g4m_tmp.gms", open="wt") #open file connection to write
writeLines(tempString, simDestination)
close(simDestination)

# Change wd to run post-processing file
wd_model <- paste0(wd,"Model/")
setwd(wd_model)

# Run post-processing script
rc <- tryCatch(
  system("gams 8_merge_output_tmp.gms"),
  error=function(e) e
  )
if(rc != 0){
  setwd(wd)
  stop(paste("Bad return from gams"))
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
tempString <- readLines("./Model/1_downscaling.gms",warn=FALSE)
tempString[2] <- paste0("$setglobal project ",project) 
tempString[3] <- paste0("$setglobal lab     ",date_label) 
 
# Save file 
simDestination <- file("./Model/1_downscaling_tmp.gms", open="wt") #open file connection to write
writeLines(tempString, simDestination)
close(simDestination)


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
  if (i==1) {scen_string <- paste0(scen_string,paste0(min(scenarios_idx),":",max(scenarios_idx)))} else {
  scen_string <- paste0(scen_string,",",paste0(min(scenarios_idx),":",max(scenarios_idx)))}
} 
scen_string <- paste0(scen_string,")")

# Update sample_config file - needs revised 1_downscaling script and 
# reorganization of Downscaling folder (All files into a Model folder and an R folder with limpopo scripts)

tempString <- readLines("./R/sample_config.R",warn=FALSE)
tempString[17] <- paste0("EXPERIMENT = \"",project,"\"# label for your run")    
tempString[19] <- paste0("JOBS = ",scen_string)

simDestination <- file("./R/sample_config.R", open="wt") #open file connection to write
writeLines(tempString, simDestination)
close(simDestination)

# Submit runs to limpopo
system("RScript R/Condor_run.R R/sample_config.R")

cluster_nr <- as.numeric(readLines(paste0("./Condor/",project,"/cluster_nr.txt"),warn=FALSE))

# Transfer gdx to G4M folder - in case files were merged on limpopo
if (merge_gdx_downscaling){
  # Read merged output
  f <- paste("./Model/gdx/downscaled_",project,"_",cluster_nr,"_merged.gdx",sep="")
  globiom4g4m_file <- gdx(f)
  all_outputs_glob4g4m <- all_items(globiom4g4m_file)
  allparam_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$parameters,f)
  allsets_glob4g4m <- batch_extract_tib(all_outputs_glob4g4m$sets,f)
  
  # Write to G4M folder
  write.gdx(paste0(path_for_G4M,"downscaled_","output_",project,"_",date_label,".gdx"), 
            params = allparam_glob4g4m, sets = allsets_glob4g4m)
}

if (!merge_gdx_downscaling & merge_regions){
  for (i in 1:length(scenarios_for_downscaling)){
    scenarios_idx <- which(scenario_mapping %in% scenarios_for_downscaling[i]) - 1
    merge_gdx_down(project,paste0(wd_downscaling,"Model/gdx"),scenarios_idx,
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




