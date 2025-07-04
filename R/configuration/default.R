#-------------------------------------------------------------------------------

# 0th block - Global options
#-------------------------------------------------------------------------------
GAMS_VERSION = "42.5" # GAMS version for HTCondor parallel runs, should match what you use locally
RUN_AS_OWNER = TRUE # Run jobs of HTCondor parallel runs as user that runs the link

#-------------------------------------------------------------------------------

# 1st block - Initial GLOBIOM run
#-------------------------------------------------------------------------------

# Limpopo scenario configuration - may include all sample_config options
WD_GLOBIOM = "GLOBIOM" # optional, working directory for GLOBIOM relative to root directory
PROJECT = "test_Link" # project name
SCENARIOS = c(0,15) # scenarios to run
GLOBIOM_RESTART_FILE = "a4_r1.g00" # restart file name from GLOBIOM
GLOBIOM_GAMS_ARGS = "//nsim=%1 //limpopo=yes //yes_output=1 //lookup=0 //ssp=SSP2 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130" # GAMS arguments for the GLOBIOM run
GLOBIOM_SCEN_FILE = "6_scenarios.gms"
GLOBIOM_POSTPROC_FILE = "8_merge_output.gms"
THERE_ARE_BTC_Scenarios = TRUE ## in the modelled scenarios, are there scenarios that include BTC (bending the curve) type of biodiversity protection & conservation setups? (This information needs to be passed on to DownScale to choose the correct initial LC map, and to G4M to prepare corresponding forest protection layers, to ensure consistent and full BTC representation throughout the GLOBIOM-G4M pipeline)
SCENARIOS_BTC <- c(0) # Change this scenario list, to include only scenarios that include the BTC setting. If all scenarios do not have BTC setting, then put SCENARIOS_BTC <- NULL.

#-------------------------------------------------------------------------------

# 2nd block - Initial post-processing
#-------------------------------------------------------------------------------

# Post-processing script configuration (8_merged_output)
WD_POSTPROC = "PostProcessing"
DATE_LABEL = format(Sys.Date(), "%d%m%Y") # date of run
REGIONAL_AG = "ggi" # regional aggregation level
REGION_RESOLUTION = 37 # regional resolution for GLOBIOM: currently two options available - 37 or 59 REGIONS

#-------------------------------------------------------------------------------

# 3rd block - Downscaling
#-------------------------------------------------------------------------------

# Downscaling configuration
DOWNSCALING_TYPE = "downscalr" # optional, one of "default", "econometric", "econometricMNL" or "downscalr"
WD_DOWNSCALING = "DownScale" # optional, working directory for downscaling relative to root directory
WD_G4M = "G4M" # optional, working directory for G4M relative to root directory
PATH_FOR_G4M = stringr::str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}") # directory where to save GDX for G4M run
SCENARIOS_FOR_DOWNSCALING = c(0,15) # full set or subset of scenarios defined previously
RESOLUTION_DOWNSCALING = REGION_RESOLUTION # number of regions specified in the downscaling

# Specific parameters for statistical downscaling (DOWNSCALING_TYPE = downscalr)
ISIMIP = FALSE # Use starting land cover maps from ISIMIP
CLUSTER = TRUE # runs downscaling on the cluster
DOWNSCALR_SCRIPT = ifelse(ISIMIP,"downscaling_GAMSlink_ISIMIP.R","downscaling_GAMSlink_GGL.R")
#-------------------------------------------------------------------------------

# 4th and 5th block - G4M run
#-------------------------------------------------------------------------------
G4M_EXE = "G4M_global_REGION37or59_Aug2024.exe" # name of G4M executable: this updated G4M version is used for either REGION37 or REGION59
BASE_SCEN1 = "SSP2" # SCEN1 to use as baseline
BASE_SCEN2 = "SCEN" # SCEN2 to use as baseline (Current default in Trunk is 'SCEN'; please adapt to specific scenario name according to PROJECT)
BASE_SCEN3 = "scenRCPref" # SCEN3 to use as baseline
G4M_SUBMISSION_SCRIPT = "g4m_run.R" # submission script to run G4M
CO2_PRICE = -1 # co2 price for G4M run, -1 if read form a file or actual price otherwise
SCENARIOS_FOR_G4M = c(0,15) # full set or subset of downscaled scenarios

#-------------------------------------------------------------------------------

# 6th block - Biodiversity report
#-------------------------------------------------------------------------------
WD_BIODIVERSITY = "Biodiversity_Link"
GET_BIODIVERSITY_INDICES = TRUE # Compute biodiversity indices
COMPUTE_BII = TRUE # compute BII index
COMPUTE_cSAR = TRUE # compute cSAR index
SCENARIOS_FOR_BIODIVERSITY = SCENARIOS_FOR_G4M # choose a full set or subset of scenarios that ran through downscaling & G4M steps
#-------------------------------------------------------------------------------


# 7th block - Final post-processing
#-------------------------------------------------------------------------------
PATH_FOR_FEEDBACK = stringr::str_glue("out/{PROJECT}_{DATE_LABEL}/") # directory for G4M output file relative to WD_G4M
G4M_FEEDBACK_FILE = stringr::str_glue("tabs_gui_{PROJECT}_{DATE_LABEL}_final_csv.csv") #Name of G4M output file
GLOBIOM_POST_FILE = "8_merge_output_tmp.gms"
USE_LIMPOPO_POSTPROC = FALSE # logical, use limpopo to run the post-processing script (use only if out file is likely to exceed the local memory)
GENERATE_PLOTS = FALSE # logical, generate plots for the GLOBIOM and lookup table results
SCENARIOs_PLOT_LOOKUP = c(0,15) # list of scenarios to include in diagnostic plots for the lookup table
SCENARIOS_PLOT_GLOBIOM = c(0,15) # list of scenarios to include in diagnostic plots for the GLOBIOM results
#-------------------------------------------------------------------------------

