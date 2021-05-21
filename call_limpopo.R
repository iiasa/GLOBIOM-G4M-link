#' 5/5/21 Andrey L. D. Augustynczik adapted the limpopo configuration file
#' and post-processing gams 8_merged_output scripts to automatize the link
#' between GLOBIOM and G4M 
#' 
#' In the first block, the code reads in and edit the limpopo submission 
#' scripts and launches GLOBIOM scenarios, post-processing and downscaling 
#' scripts in sequence. In the second block,... to be done
#' the code calls G4M. In the third block,... to be done


################################################################################
#                        CONFIGURATION PROCESSING
################################################################################

# Remove any objects from active environment and read the default configuration
rm(list=ls())
source("R/default_configuration.R")

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
        name != "RESOLUTION_DOWNSCALING" && # R has no stable numerical type
        type != "NULL" && # allow for configured vector being empty
        config_types[[i]] != "NULL" # allow for default vector being empty
    ) stop(str_glue("{name} set to wrong type in {args[1]}, type should be {config_types[[i]]}"))
  }
} else {
  stop("Multiple arguments provided! Expecting at most a single config file argument.")
}

# R temporary directory

TEMP_DIR <- tempdir()
fsep <- ifelse(str_detect(TEMP_DIR, fixed("\\") ), "\\", ".Platform$file.sep") # Get the platform file separator: .Platform$file.sep is set to / on Windows
TEMP_DIR <- str_replace_all(TEMP_DIR, fixed(fsep), .Platform$file.sep)
rm(fsep)

################################################################################
#                                GLOBIOM-G4M-LINK
################################################################################


# Load link functions

cd <- getwd()
source("R/globiom_g4m_functions.R")
source("R/helper_functions.R")

run_link <- function()
{
  
  # Initial globiom run
  if (GLOBIOM_INITIAL) 
  {
    
    # Run GLOBIOM
    run_globiom_initial(cd)
    
    print("Initial GLOBIOM run complete")
  }
  
  if (DOWNSCALING_INITIAL) 
  {
    # Run Initial downscaling
    
    if (!file.exists(str_glue(WD_DOWNSCALING,"/input/","output_landcover_",PROJECT,"_",DATE_LABEL,".gdx"))) stop("File for downscaling not found! Please call the intial GLOBIOM run before downscaling")
  
    run_downscaling(cd)
    
    print("Initial downscaling complete")
  }
  
  if (G4M) 
  {
    if (!file.exists(str_glue(PATH_FOR_G4M,"/",GDX_OUTPUT_NAME,"_output_",PROJECT,"_",DATE_LABEL))) stop("File for G4M run not found! Please call the intial downscaling before the G4M run")
    
    # Run G4M

  }
  
  if (GLOBIOM_FINAL) 
  {
    # Run GLOBIOM

  }
  
  if (DOWNSCALING_FINAL) 
  {
    # Run GLOBIOM
    
  }
}

run_link()
