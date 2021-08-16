# Functions for submission of runs of parallel jobs to a Condor cluster
# so as to speed up processing.

#' Run GLOBIOM scenarios
#'
#' Run the initial GLOBIOM scenarios by submitting the configured scenarios for
#' parallel execution on an HTCondor cluster.
#'
#' @return cluster_nr Cluster sequence number of HTCondor submission
run_globiom_scenarios <- function() {

  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({str_c(SCENARIOS, collapse=",")})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 13000',
    'REQUEST_CPUS = 1',
    'GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "{GLOBIOM_SCEN_FILE}"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//nsim=%1 //yes_output=1 //ssp=SSP2 //scen_type=feedback //water_bio=0 PC=2 PS=0 PW=130"',
    'BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RESTART_FILE_PATH = "t/a4_r1.g00"',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "a6_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "output.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = {MERGE_GDX}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"',
    'CLEAR_LINES = FALSE'
  )

  config_path <- path(TEMP_DIR, "config_glob.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Submit GLOBIOM scenarios and wait for run completion
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_GLOBIOM)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}

#' Run initial downscaling
#'
#' Run the initial downscaling by submitting scenarios for parallel execution on
#' an HTCondor cluster.
#'
#' @return cluster_nr Cluster sequence number of HTCondor submission
run_initial_downscaling <- function() {

  # Check if input data is empty
  downs_input <- file_size(path(CD, WD_DOWNSCALING, "input", str_glue("output_landcover_{PROJECT}_{DATE_LABEL}.gdx")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")

  # Get globiom and downscaling scenario mapping
  scenario_mapping <- get_mapping() # Matching by string for now - should come directly from globiom in the future

  # Define scenarios for downscaling
  downscaling_scenarios <- subset(scenario_mapping, ScenLoop %in% SCENARIOS_FOR_DOWNSCALING)

  # Define downscaling scenarios for limpopo run
  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[i])]
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  }
  scen_string <- str_glue(scen_string,")")

  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'PREFIX = "_globiom"',
    'JOBS = c({scen_string})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 2500',
    'REQUEST_CPUS = 1',
    'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "//project=\\\"{PROJECT}\\\" //lab=\\\"{DATE_LABEL}\\\" //gdx_path=\\\"gdx/{GDX_OUTPUT_NAME}.gdx\\\" //nsim=%1"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c(".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst")',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'G00_OUTPUT_DIR = "t"',
    'G00_OUTPUT_FILE = "d1_out.g00"',
    'GET_G00_OUTPUT = FALSE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "downscaled.gdx"',
    'GET_GDX_OUTPUT = TRUE',
    'MERGE_GDX_OUTPUT = TRUE',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"',
    'CLEAR_LINES = FALSE'
  )
  config_path <- file.path(TEMP_DIR, "config_down.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  # Submit downscaling run and wait for run completion
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_DOWNSCALING)
    # Ensure that required directories exist
    if (!dir_exists('gdx')) dir_create("gdx")
    if (!dir_exists('output')) dir_create("output")
    if (!dir_exists('t')) dir_create("t")

    system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}

#' Run G4M
#'
#' Run G4M by submitting jobs for parallel execution on an HTCondor cluster.
#'
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
run_g4m <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  # Configure and run scenarios using Condor_run.R

  # Check if input data is empty
  downs_input <- file_size(path(WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")
  glob_input <- file_size(path(WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx")))
  if (glob_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")

  #g4m_jobs <- get_g4m_jobs(baseline = baseline)[-1] # EPA files for testing
  g4m_jobs <- get_g4m_jobs_new(baseline = baseline)[-1] # implementation for the new G4M interface
  if (length(g4m_jobs) == 1) g4m_jobs <- str_glue('"{g4m_jobs}"')

  config_template <- c(
    'PROJECT = "{PROJECT}"',
    'PREFIX = "g4m"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'G4M_EXE = "{G4M_EXE}"',
    'JOBS =',
    '{g4m_jobs}',
    'SEED_FILES = ""',
    'GAMS_CURDIR = ""', # optional, working directory for GAMS and its arguments relative to working directory, "" defaults to the working directory
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 3000',
    'REQUEST_CPUS = 1',
    'BUNDLE_INCLUDE = "*"',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'BASELINE_RUN = {baseline}'
  )

  config_path <- path(TEMP_DIR, "config_g4m.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_G4M)
    # Ensure that a sub directories for run logs and outputs exist
    if (!dir_exists('Condor')) dir_create("Condor")

    system(str_glue("Rscript --vanilla {CD}/{WD_G4M}/R/Condor_run.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")
}

# vvvv Below follows a partial attempt to get rid of customized G4M/RCondor_run.R

# Define the job template for G4M. Need to double escape since this
# is parsed twice, here and in the generated configuration file.
# Note that {...} instances are escaped as {{...}},
# Note that as of R 4.0.0, r(...) raw string constants are an option,
# but we want to support R < 4.0.0
G4M_JOB_TEMPLATE <- "c(
  \'Executable = {{job_bat}}\',
  \"\",
  \'Universe = vanilla\',
  \"\",
  \'output = {{run_dir}}/{{PREFIX}}_{{LABEL}}_$(Cluster).$(Process).out\',
  \'log = {{run_dir}}/{{PREFIX}}_{{LABEL}}_$(Cluster).$(Process).log\',
  \'error = {{run_dir}}/{{PREFIX}}_{{LABEL}}_$(Cluster).$(Process).err\',
  \"\",
  \'## To protect the job from running on the same machine\',
  \'## where it crashed before\',
  \'job_machine_attrs = Machine\',
  \'job_machine_attrs_history_length = 5\',
  \"\",
  \'+IIASAGroup = \"ESM\"\',
  \"\",
  \"requirements = \\\\\",
  \'  ( (Arch ==\"INTEL\")||(Arch ==\"X86_64\") ) && \\\\\',
  \'  ( (OpSys == \"WINDOWS\")||(OpSys == \"WINNT61\") ) && \\\\\',
  \"  ( ( TARGET.Machine == \\\"{{str_c(hostdoms, collapse=\'\\\" ) || ( TARGET.Machine == \\\"\')}}\\\") )\",
  \"\",
  \'request_memory = {{REQUEST_MEMORY}}\',
  \'request_cpus = 1\',
  \'rank = mips\',
  \"\",
  \'## Hold the suspended job (i.e. do not allow suspention)\',
  \'periodic_hold = (JobStatus == 7)\',
  \'periodic_release = (NumJobStarts <= 10) && (HoldReasonCode != 1) && ((time() - EnteredCurrentStatus)>300)\',
  \"\",
  \'##  specifies how long a given job will attempt to run on a remote resource, even if that resource loses contact with the submitting machine, seconds, default=2400 (40 minutes)\',
  \'job_lease_duration = 18000\',
  \"\",
  \'## End the job only when exitcode = 0\',
  \'on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\',
  \"\",
  \'## Hold the job if NumJobStarts > 10\',
  \'on_exit_hold = (NumJobStarts > 10) && (ExitCode != 0)\',
  \"\",
  \'should_transfer_files = YES\',
  \'when_to_transfer_output = ON_EXIT\',
  \'stream_output = True\',
  \'stream_error = True\',
  \'transfer_output_files = out\',
  \'transfer_output_remaps = \"out={output_dir}\"\',
  \'Notification = Error\',
  \"\",
  \"queue arguments from (\",
  {{JOBS}},
  \")\"
)
"

#' Run G4M
#'
#' Run G4M by submitting jobs for parallel execution on an HTCondor cluster.
#'
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
run_g4m_attempt <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  # Provide and output directory relative to WD_G4M
  if (baseline) {
    output_dir <- path("out", str_glue("{PROJECT}_{DATE_LABEL}/baseline"))
  } else {
    output_dir <- path("out", str_glue("{PROJECT}_{DATE_LABEL}"))
  }
  if (!dir_exists(path(WD_G4M, output_dir)))
    dir_create(path(WD_G4M, output_dir))

  # Determine files for bundle
  default_file_list <- dir_ls(path_wd(WD_G4M, "Data", "Default"))
  glob_file_list <- dir_ls(path_wd(WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}")))
  base_file_list <- c(default_file_list, glob_file_list)
  if (baseline) {
    seed_files <- c(base_file_list, str_glue("{G4M_EXE}"))
    seed_files <- seed_files[which(!str_detect(seed_files, ".gdx"))]

  } else {
    bau_file_list <- dir_ls(path_wd(WD_G4M, "out", str_glue("{PROJECT}_{DATE_LABEL}"), "baseline"))
    idx <- which(str_detect(bau_file_list, "biomass_bau") | str_detect(bau_file_list, "NPVbau"))
    bau_file_list <- bau_file_list[idx]
    seed_files <- c(base_file_list, bau_file_list, str_glue("{G4M_EXE}"))
  }

  # Configure and run scenarios using Condor_run.R

  # Check if input data is empty
  downs_input <- file_size(path(WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")
  glob_input <- file_size(path(WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx")))
  if (glob_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")

  #g4m_jobs <- get_g4m_jobs(baseline = baseline)[-1] # EPA files for testing
  g4m_jobs <- get_g4m_jobs_new(baseline = baseline)[-1] # implementation for the new G4M interface
  if (length(g4m_jobs) == 1) g4m_jobs <- str_glue('"{g4m_jobs}"')

  config_template <- c(
    'PROJECT = "{PROJECT}"',
    'PREFIX = "g4m"',
    'JOBS =',
    '{g4m_jobs}',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 3000',
    'REQUEST_CPUS = 1',
    'LAUNCHER = "{G4M_EXE}"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'BUNDLE_INCLUDE = "{seed_files}"',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'JOB_TEMPLATE = {G4M_JOB_TEMPLATE}'
  )

  config_path <- path(TEMP_DIR, "config_g4m.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_G4M)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R//Condor_run_basic.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")
}
