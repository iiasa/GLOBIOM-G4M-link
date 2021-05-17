# Sample configuration file for submitting Condor jobs to limpopo,
# set up to work with what is in the Trunk.
#
# First run the pre-compilation using Data/0_executebatch_total.gms
# and then run Model/0_executebatch.gms up to but excluding step 6
# using a GAMS version equal or older than what is configured via
# GAMS_VERSION (see below). This produces the required restart
# file configured via RESTART_FILE_PATH.
#
# Then submit the scenario jobs with this configuration  by making
#the GLOBIOM root directory (holding Data, Model, R, and so on) the
# current working directory and then invoking:
# mkdir Condor
# RScript R/Condor_run.R R/sample_config.R
#
# Please read the header of R/Condor_run.R for further information.
EXPERIMENT = "test_EPA"# label for your run
PREFIX = "_globiom" # prefix for per-job .err, log, .lst, and .out files
JOBS = c(c(0,75))
HOST_REGEXP = "^limpopo[1-6]" # a regular expression to select execute hosts from the cluster
REQUEST_MEMORY = 13000 # memory (MiB) to reserve for each job
REQUEST_CPUS = 1 # number of hardware threads to reserve for each job
GAMS_CURDIR = "Model" # optional, working directory for GAMS and its arguments relative to working directory, "" defaults to the working directory
GAMS_FILE_PATH = "6_scenarios.gms" # path to GAMS file to run for each job, relative to GAMS_CURDIR
GAMS_VERSION = "32.2" # must be installed on all execute hosts
GAMS_ARGUMENTS = "//nsim='%1' //yes_output=1 //ssp=SSP2 //scen_type=p_driven //price_exo=0 //dem_fix=0 //irri_dem=1 //water_bio=0 PC=2 PS=0 PW=130" # additional GAMS arguments, can use {<config>} expansion here
BUNDLE_INCLUDE = "Model" # optional, recursive, what to include in bundle, can be a wildcard
BUNDLE_INCLUDE_DIRS = c("include") # optional, further directories to include recursively, added to root of bundle, supports wildcards
BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc") # optional, recursive, supports wildcards
BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst") # optional, supports wildcards
BUNDLE_ADDITIONAL_FILES = c() # optional, additional files to add to root of bundle, can also use an absolute path for these
RETAIN_BUNDLE = FALSE # optional
RESTART_FILE_PATH = "t/a4_r1.g00" # optional, included in bundle if set, relative to GAMS_CURDIR
G00_OUTPUT_DIR = "t" # relative to GAMS_CURDIR both host-side and on the submit machine, excluded from bundle
G00_OUTPUT_FILE = "a6_out.g00" # host-side, will be remapped with EXPERIMENT and cluster/job numbers to avoid name collisions when transferring back to the submit machine.
GET_G00_OUTPUT = FALSE
GDX_OUTPUT_DIR = "gdx" # relative to GAMS_CURDIR both host-side and on the submit machine, excluded from bundle
GDX_OUTPUT_FILE = "output.gdx" # as produced on the host-side by gdx= GAMS parameter or execute_unload, will be remapped with EXPERIMENT and cluster/job numbers to avoid name collisions when transferring back to the submit machine.
GET_GDX_OUTPUT = TRUE
MERGE_GDX_OUTPUT = TRUE # optional
WAIT_FOR_RUN_COMPLETION = TRUE
NICE_USER = FALSE # optional, be nice, give jobs of other users priority
