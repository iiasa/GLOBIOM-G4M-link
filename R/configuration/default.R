# 1st block - Initial GLOBIOM run
#-------------------------------------------------------------------------------

# Limpopo scenario configuration - may include all sample_config options
WD_GLOBIOM = "GLOBIOM" # optional, working directory for GLOBIOM relative to root directory
PROJECT = "test_Link" # project name
SCENARIOS = 0 # scenarios to run
MERGE_GDX = TRUE # merge gdx output on limpopo
GLOBIOM_RESTART_FILE = "a4_r1.g00" # restart file name from GLOBIOM
GLOBIOM_GAMS_ARGS = "//nsim=%1 //yes_output=1 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130" # GAMS arguments for the GLOBIOM run
GLOBIOM_SCEN_FILE = "6_scenarios.gms"
MERGE_GDX_CUTOFF = FALSE # use big parameter in gdxmerge, recommended in case of a large number of scenarios (if activated will use big = 1000000)

# Post-processing script configuration (8_merged_output)
LIMPOPO_RUN = "yes" # Run on limpopo yes/no
#RESOLUTION = "region37" # set GLOBIOM region "resolution"
DATE_LABEL = format(Sys.Date(), "%d%m%Y") # date of run
REPORTING_G4M = "yes" # reporting to G4M yes/no
REPORTING_IAMC = "yes" # reporting to IAMC yes/no
REPORTING_IAMC_G4M = "no" # reporting G4M to IAMC REGIONAL_AG/no
REGIONAL_AG = "ggi" # regional aggregation level

#-------------------------------------------------------------------------------

# 2nd block - Downscaling
#-------------------------------------------------------------------------------

# Downscaling configuration
DOWNSCALING_TYPE = "default" # optional, one of "default", "econometric", or "econometricMNL"
WD_DOWNSCALING = "DownScale" # optional, working directory for downscaling relative to root directory
WD_G4M = "G4M" # optional, working directory for G4M relative to root directory
MERGE_GDX_DOWNSCALING = FALSE # merge all gdx outputs on limpopo
GDX_OUTPUT_NAME = "downscaled" # prefix of downscaled gdx file
MERGE_REGIONS = FALSE # merge gdx locally by scenario
PATH_FOR_G4M = stringr::str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}") # directory where to save GDX for G4M run
SCENARIOS_FOR_DOWNSCALING = 0 # full set or subset of scenarios defined previously
RESOLUTION_DOWNSCALING = 37 # number of regions specified in the downscaling

#-------------------------------------------------------------------------------

# 3rd block - G4M run
#-------------------------------------------------------------------------------
G4M_EXE = "G4M_newInterface_ver_EPA_07052021.exe" # name of G4M executable
G4M_SUBMISSION_SCRIPT = "g4m_run.R" # submission script to run G4M
CO2_PRICE = -1 # co2 price for G4M run, -1 if read form a file or actual price otherwise
SCENARIOS_FOR_G4M = 0 # full set or subset of downscaled scenarios

#-------------------------------------------------------------------------------

# 4rd block - final post-processing
#-------------------------------------------------------------------------------
PATH_FOR_FEEDBACK = stringr::str_glue("out/{PROJECT}_{DATE_LABEL}/") # directory for G4M output file relative to WD_G4M
G4M_FEEDBACK_FILE = stringr::str_glue("tabs_gui_{PROJECT}_{DATE_LABEL}_final_csv.csv") #Name of G4M output file
REPORTING_G4M_FINAL = "no" # reporting to G4M yes/no
REPORTING_IAMC_FINAL = "yes" # reporting to IAMC yes/no
REPORTING_IAMC_G4M_FINAL = "yes" #  reporting G4M to IAMC REGIONAL_AG/no
GLOBIOM_POST_FILE = "8_merge_output_tmp.gms" # reporting script
USE_LIMPOPO_POSTPROC = FALSE # logical, use limpopo to run the post-processing script (use only if out file is likely to exceed the local memory)
BASE_SCEN1 = "SSP2" # SCEN1 to use as reference for forest management emissions
BASE_SCEN2 = "SPA0" # SCEN2 to use as reference for forest management emissions
BASE_SCEN3 = "scenRCPref" # SCEN3 to use as reference for forest management emissions
#-------------------------------------------------------------------------------
