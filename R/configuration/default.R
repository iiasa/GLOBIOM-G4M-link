# 1st block - Initial GLOBIOM run
#-------------------------------------------------------------------------------

# Limpopo scenario configuration - may include all sample_config options
WD_GLOBIOM = "GLOBIOM" # optional, working directory for GLOBIOM relative to root directory
PROJECT = "test_Link" # project name
SCENARIOS = c(0,15) # scenarios to run
GLOBIOM_RESTART_FILE = "a4_r1.g00" # restart file name from GLOBIOM
GLOBIOM_GAMS_ARGS = "//nsim=%1 //yes_output=1 //ssp=SSP2 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130" # GAMS arguments for the GLOBIOM run
GLOBIOM_SCEN_FILE = "6_scenarios.gms"
GLOBIOM_POSTPROC_FILE = "8_merge_output.gms"

#-------------------------------------------------------------------------------

# 2nd block - Initial post-processing
#-------------------------------------------------------------------------------

# Post-processing script configuration (8_merged_output)
DATE_LABEL = format(Sys.Date(), "%d%m%Y") # date of run
REGIONAL_AG = "ggi" # regional aggregation level

#-------------------------------------------------------------------------------

# 3rd block - Downscaling
#-------------------------------------------------------------------------------

# Downscaling configuration
DOWNSCALING_TYPE = "default" # optional, one of "default", "econometric", or "econometricMNL"
WD_DOWNSCALING = "DownScale" # optional, working directory for downscaling relative to root directory
WD_G4M = "G4M" # optional, working directory for G4M relative to root directory
PATH_FOR_G4M = stringr::str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}") # directory where to save GDX for G4M run
SCENARIOS_FOR_DOWNSCALING = c(0,15) # full set or subset of scenarios defined previously
RESOLUTION_DOWNSCALING = 37 # number of regions specified in the downscaling

#-------------------------------------------------------------------------------

# 4th and 5th block - G4M run
#-------------------------------------------------------------------------------
G4M_EXE = "G4M_newInterface_ver_EPA_07052021.exe" # name of G4M executable
BASE_SCEN1 = "SSP2" # SCEN1 to use as baseline
BASE_SCEN2 = "SPA0" # SCEN2 to use as baseline
BASE_SCEN3 = "scenRCPref" # SCEN3 to use as baseline
G4M_SUBMISSION_SCRIPT = "g4m_run.R" # submission script to run G4M
CO2_PRICE = -1 # co2 price for G4M run, -1 if read form a file or actual price otherwise
SCENARIOS_FOR_G4M = c(0,15) # full set or subset of downscaled scenarios

#-------------------------------------------------------------------------------

# 6th block - Final post-processing
#-------------------------------------------------------------------------------
PATH_FOR_FEEDBACK = stringr::str_glue("out/{PROJECT}_{DATE_LABEL}/") # directory for G4M output file relative to WD_G4M
G4M_FEEDBACK_FILE = stringr::str_glue("tabs_gui_{PROJECT}_{DATE_LABEL}_final_csv.csv") #Name of G4M output file
GLOBIOM_POST_FILE = "8_merge_output_tmp.gms"
USE_LIMPOPO_POSTPROC = TRUE # logical, use limpopo to run the post-processing script (use only if out file is likely to exceed the local memory)
GENERATE_PLOTS = FALSE # logical, generate plots for the GLOBIOM and lookup table results
SCENARIOs_PLOT_LOOKUP = c(0,15) # list of scenarios to include in diagnostic plots for the lookup table
SCENARIOS_PLOT_GLOBIOM = c(0,15) # list of scenarios to include in diagnostic plots for the GLOBIOM results
#-------------------------------------------------------------------------------
