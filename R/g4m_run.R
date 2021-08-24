
args <- commandArgs(trailing = TRUE)

# Load libraries
library(dplyr)
library(stringr)

# Load configuration data and scenario map
load("scenario_map.RData")
load("config.RData")

# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("At least one argument must be supplied", call.=FALSE)
} else {
  # default output file
  scen <- as.integer(args[1])
}

# Retrieve configuration arguments
lab <- config[[1]]
baseline <- config[[2]]
g4m_exe <- config[[3]]

# Get co2 price
CO2P <- 0
if (!baseline) CO2P = -1

# Filter scenario attributes
g4m_scen_table <- downs_map %>% filter(ScenLoop==scen)

# Baseline scenario
sb_str <- str_c(g4m_scen_table$MacroScen,"_",g4m_scen_table$IEAScen,"_",g4m_scen_table$BioenScen)
# G4M Scenario - For now the same as baseline but may be tailored in the future
s_str <- str_c(g4m_scen_table$MacroScen,"_",g4m_scen_table$IEAScen,"_",g4m_scen_table$BioenScen)

# Run g4m
system2(g4m_exe, args=c(lab,sb_str,sb_str,CO2P))
