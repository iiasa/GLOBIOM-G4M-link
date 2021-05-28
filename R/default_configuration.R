################################################################################
#                     DEFAULT CONFIGURATION PARAMETERS
################################################################################

# 1st block - Initial GLOBIOM run
#-------------------------------------------------------------------------------

# Limpopo scenario configuration - may include all sample_config options
WD_GLOBIOM = "GLOBIOM" # working directory for GLOBIOM relative to current path
PROJECT = "test_EPA" # project name
SCENARIOS = "0" # scenarios to run
MERGE_GDX = TRUE # merge gdx output on limpopo

# Post-processing script configuration (8_merged_output)
LIMPOPO_RUN = "yes" # Run on limpopo yes/no
RESOLUTION = "region37" # Set GLOBIOM region "resolution"
DATE_LABEL = format(Sys.Date(), "%d%m%Y") # Date of run
REPORTING_G4M = "yes" #Reporting to G4M yes/no
REPORTING_IAMC = "no" #Reporting to IAMC yes/no
REPORTING_IAMC_G4M = "no" # Reporting G4M to IAMC REGIONAL_AG/no 
REGIONAL_AG = "ggi" # regional aggregation level
PATH_FOR_DOWNSCALING = "H:/Downscaling/Model/input/" # path to save gdx for downscaling

# Downscaling configuration
WD_DOWNSCALING = "DownScale" # working directory for downscaling relative to current path
MERGE_GDX_DOWNSCALING = TRUE # merge all gdx outputs on limpopo
GDX_OUTPUT_NAME = "downscaled" # prefix of downscaled gdx file
MERGE_REGIONS = FALSE # merge gdx locally by scenario 
PATH_FOR_G4M = "H:/Downscaling/Model/output/" # path to save gdx for G4M run
SCENARIOS_FOR_DOWNSCALING = "0" # full set or subset of scenarios defined previously
RESOLUTION_DOWNSCALING = 37 # number of regions specified in the downscaling

#-------------------------------------------------------------------------------

# 2nd block - G4M run
#-------------------------------------------------------------------------------

#-------------------------------------------------------------------------------

# 3rd block - final GLOBIOM run
#-------------------------------------------------------------------------------
PATH_FOR_FEEDBACK = "I:/augustynczik/" #Path for G4M output file
G4M_FEEDBACK_FILE = "tabs_gui_FAOFRA2015CRF_CSIRO_t14_SSP2_EPA_07052021_final_csv_test3.csv" #Name of G4M output file
REPORTING_G4M_FINAL = "no" #Reporting to G4M yes/no
REPORTING_IAMC_FINAL = "yes" #Reporting to IAMC yes/no
REPORTING_IAMC_G4M_FINAL = "yes" # Reporting G4M to IAMC REGIONAL_AG/no 
#-------------------------------------------------------------------------------
