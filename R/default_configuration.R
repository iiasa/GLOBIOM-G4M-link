################################################################################
#                     DEFAULT CONFIGURATION PARAMETERS
################################################################################

# Global options
#-------------------------------------------------------------------------------

globiom_intial <- T # call the initial GLOBIOM run
downscaling_initial <- T # call the initial downscaling - needs data from the intial GLOBIOM run
G4M <- F # call G4M - needs data from the intial downscaling
globiom_final <- F # call the final GLOBIOM run - needs data from the G4M run
downscaling_final <- F # call the final downscaling - needs data from the final GLOBIOM run

#-------------------------------------------------------------------------------


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
date_label <- format(Sys.Date(), "%d%m%Y") # Date of run
reporting_g4m <- "yes" #Reporting to G4M yes/no
reporting_iamc <- "yes" #Reporting to IAMC yes/no
reporting_iamc_g4m <- "no" # Reporting G4M to IAMC yes/no 
g4m_feedback_file <- stringr::str_glue("tabs_gui_FAOFRA2015CRF_CSIRO_t14_SSP2_iea_REGION37_",
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
