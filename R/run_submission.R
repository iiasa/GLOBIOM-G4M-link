# Functions for submission of runs of parallel jobs to a Condor cluster
# so as to speed up processing.

#' Run GLOBIOM scenarios
#'
#' Run the initial GLOBIOM scenarios by submitting the configured scenarios for
#' parallel execution on an HTCondor cluster.
#'
#' @return cluster_nr Cluster sequence number of HTCondor submission
run_globiom_scenarios <- function() {

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
  config_template <- c(
    'LABEL = "{PROJECT}"',
    'JOBS = c({str_c(SCENARIOS, collapse=",")})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 18000',
    'REQUEST_DISK = 2200000',
    'REQUEST_CPUS = 1',
    'GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "{GLOBIOM_SCEN_FILE}"',
    'GAMS_VERSION = "32.2"',
    'GAMS_ARGUMENTS = "{GLOBIOM_GAMS_ARGS}"',
    'BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c("Model/t",".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst","**/output/iamc/*.*","**/output/g4m/*.*","**/gdx/*.*") # supports wildcards',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RESTART_FILE_PATH = "t/{GLOBIOM_RESTART_FILE}"',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_GDX_OUTPUT = TRUE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "output.gdx"',
    'MERGE_GDX_OUTPUT = TRUE',
    'MERGE_BIG = 1000000',
    'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
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


# Define the job template for Downscaling.
# Note that as of R 4.0.0, r(...) raw string constants are an option,
# but we want to support R < 4.0.0

DOWNSCALING_JOB_TEMPLATE <- c(
  "executable = {bat_path}",
  "arguments = $(job)",
  "universe = vanilla",
  "",
  "nice_user = {ifelse(NICE_USER, 'True', 'False')}",
  "",
  "# Job log, output, and error files",
  "log = {log_dir}/{PREFIX}_$(cluster).$(job).log", # don't use $$() expansion here: Condor creates the log file before it can resolve the expansion
  "output = {log_dir}/{PREFIX}_$(cluster).$(job).out",
  "stream_output = True",
  "error = {log_dir}/{PREFIX}_$(cluster).$(job).err",
  "stream_error = True",
  "", # If a job goes on hold for more than JOB_RELEASE_DELAY seconds, release it up to JOB_RELEASES times
  "periodic_release =  (NumJobStarts <= {JOB_RELEASES}) && (JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > {JOB_RELEASE_DELAY})",
  "periodic_remove =  (JobStatus == 5)",
  "",
  "requirements = \\",
  '  ( (Arch =="INTEL")||(Arch =="X86_64") ) && \\',
  '  ( (OpSys == "WINDOWS")||(OpSys == "WINNT61") ) && \\',
  "  ( GLOBIOM =?= True ) && \\",
  "  ( ( TARGET.Machine == \"{str_c(hostdoms, collapse='\" ) || ( TARGET.Machine == \"')}\") )",
  "request_memory = {REQUEST_MEMORY}",
  "request_cpus = {REQUEST_CPUS}", # Number of "CPUs" (hardware threads) to reserve for each job
  "request_disk = {request_disk}",
  "",
  '+IIASAGroup = "ESM"', # Identifies you as part of the group allowed to use ESM cluster
  "run_as_owner = {ifelse(RUN_AS_OWNER, 'True', 'False')}",
  "",
  "should_transfer_files = YES",
  "when_to_transfer_output = ON_EXIT",
  'transfer_output_files = {str_sub(in_gams_curdir(GAMS_FILE_PATH), 1, -5)}.lst{ifelse(GET_G00_OUTPUT, str_glue(",{in_gams_curdir(G00_OUTPUT_DIR)}/{G00_OUTPUT_FILE}"), "")}{ifelse(GET_GDX_OUTPUT, str_glue(",{in_gams_curdir(GDX_OUTPUT_DIR)}/{GDX_OUTPUT_FILE}"), "")}',
  'transfer_output_remaps = "{str_sub(GAMS_FILE_PATH, 1, -5)}.lst={log_dir}/{PREFIX}_$(cluster).$(job).lst{ifelse(GET_G00_OUTPUT, str_glue(";{G00_OUTPUT_FILE}={G00_OUTPUT_DIR_SUBMIT}/{g00_prefix}_{LABEL}_$(cluster).$(job).g00"), "")}{ifelse(GET_GDX_OUTPUT, str_glue(";{GDX_OUTPUT_FILE}={GDX_OUTPUT_DIR_SUBMIT}/{gdx_prefix}_{LABEL}_$(cluster).$$([substr(strcat(string(0),string(0),string(0),string(0),string(0),string(0),string($(job))),-6)]).gdx"), "")}"',
  "",
  "notification = {NOTIFICATION}",
  '{ifelse(is.null(EMAIL_ADDRESS), "", str_glue("notify_user = {EMAIL_ADDRESS}"))}',
  "",
  "queue job in ({str_c(JOBS,collapse=',')})"
)


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
  downscaling_scenarios <- scenario_mapping %>% filter(ScenLoop %in% SCENARIOS_FOR_DOWNSCALING)

  # Define downscaling scenarios for limpopo run
  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[i])]
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  }
  scen_string <- str_glue(scen_string,")")

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  # Check if cross-entropy optimization or statistical downscaling is performed
  if (!DOWNSCALING_TYPE == "downscalr") {

    # Cross-entropy optimization variant

    # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
    config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = c({scen_string})',
      'HOST_REGEXP = "^limpopo"',
      'REQUEST_MEMORY = 2500',
      'REQUEST_DISK = 1400000',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 3',
      'JOB_RELEASE_DELAY = 120',
      'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
      'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
      'GAMS_VERSION = "32.2"',
      'GAMS_ARGUMENTS = "//project={PROJECT} //lab={DATE_LABEL} //gdx_path=gdx/downscaled.gdx //nsim=%1"',
      'BUNDLE_INCLUDE_DIRS = c("include")',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_GDX_OUTPUT = TRUE',
      'GDX_OUTPUT_DIR = "gdx"',
      'GDX_OUTPUT_FILE = "downscaled.gdx"',
      'JOB_TEMPLATE = ',
      '{DOWNSCALING_JOB_TEMPLATE}',
      'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
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

    if (rc != 0) {
      # Try to re-run infeasible scenarios with econometric downscaling

      # Retrieve GAMS return codes from submitted jobs
      run_dir <- path(CD, WD_DOWNSCALING, "Condor", str_glue("{PROJECT}"))
      cluster <- readr::parse_number(read_file(cluster_number_log))
      scens <- eval(parse(text = scen_string))
      return_values <-
        get_return_values(run_dir, lapply(scens, function(job)
          return(
            str_glue("job_{cluster}.{job}.log")
          )))

      # Update downscaling script
      DOWNSCALING_SCRIPT = "1_downscalingEconometric.gms"

      # Get infeasible jobs
      infeasible_jobs <- scens[which(return_values != 0 | is.na(return_values))]
      scen_string <- str_c(infeasible_jobs,collapse=",")

      # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
      config_template <- c(
        'LABEL = "{PROJECT}"',
        'JOBS = c({scen_string})',
        'HOST_REGEXP = "^limpopo[5-6]"',
        'REQUEST_MEMORY = 2500',
        'REQUEST_DISK = 1400000',
        'REQUEST_CPUS = 1',
        'JOB_RELEASES = 0',
        'JOB_RELEASE_DELAY = 120',
        'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
        'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
        'GAMS_VERSION = "32.2"',
        'GAMS_ARGUMENTS = "//project={PROJECT} //lab={DATE_LABEL} //gdx_path=gdx/downscaled.gdx //nsim=%1"',
        'BUNDLE_INCLUDE_DIRS = c("include")',
        'WAIT_FOR_RUN_COMPLETION = TRUE',
        'CLEAR_LINES = FALSE',
        'GET_GDX_OUTPUT = TRUE',
        'GDX_OUTPUT_DIR = "gdx"',
        'GDX_OUTPUT_FILE = "downscaled.gdx"',
        'JOB_TEMPLATE = ',
        '{DOWNSCALING_JOB_TEMPLATE}',
        'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
      )
      config_path <- file.path(TEMP_DIR, "config_down.R")

      # Re-submit infeasible jobs
      current_env <- environment()
      write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
      rm(config_template, current_env)

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

      # If still infeasible stop
      if (rc != 0) stop("Condor run failed!")

      # Harmonize file naming
      current_cluster <- readr::parse_number(read_file(cluster_number_log))

      lapply(dir_ls(
        path(CD, WD_DOWNSCALING, "gdx"),
        regexp = str_glue("downscaled_{PROJECT}_{current_cluster}.*.gdx")
      ),
      function(x) {
        scen_nr <- str_sub(x, -10, -5)

        file_move(x, path(
          CD,
          WD_DOWNSCALING,
          "gdx",
          str_glue("downscaled_{PROJECT}_{cluster}.{scen_nr}.gdx")
        ))

      })

      # Set cluster number to original value
      write_file(as.character(cluster),cluster_number_log)

    }

  } else {

    # Statistical downscaling variant

    # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
    config_template <- c(
      'EXPERIMENT = "{PROJECT}"',
      'JOBS = c({scen_string})',
      'HOST_REGEXP = "^limpopo"',
      'REQUEST_MEMORY = 7500',
      'REQUEST_DISK = 1500000',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 3',
      'JOB_RELEASE_DELAY = 120',
      'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "{DOWNSCALR_SCRIPT}"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'BUNDLE_INCLUDE = "*"',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_OUTPUT = TRUE',
      'OUTPUT_DIR = "gdx"',
      'OUTPUT_FILE = "output.RData"',
      'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
    )

    config_path <- file.path(TEMP_DIR, "config_down.R")

    # Write config file
    current_env <- environment()
    write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
    rm(config_template, current_env)

    # Submit downscaling run and wait for run completion
    prior_wd <- getwd()

    # Save scenario parameters
    downscaling_pars <- list()

    downscaling_pars[[1]] <- ISIMIP
    downscaling_pars[[2]] <- CLUSTER
    downscaling_pars[[3]] <- PROJECT
    downscaling_pars[[4]] <- DATE_LABEL
    downscaling_pars[[5]] <- scenario_mapping

    saveRDS(downscaling_pars,path(CD,WD_DOWNSCALING,"downscaling_pars.RData"))


    rc <- tryCatch ({
      setwd(WD_DOWNSCALING)
      # Ensure that required directories exist
      if (!dir_exists('gdx')) dir_create("gdx")
      if (!dir_exists('output')) dir_create("output")
      if (!dir_exists('t')) dir_create("t")

      # Check that downscalr package is available and install
      if (!dir_exists(renv::paths$library())) {
        renv::init()
        renv::install("tkrisztin/downscalr")
      }

      system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run_basic.R {config_path}"))
    },
    finally = {
      setwd(prior_wd)
    })

  }

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}

# Define the job template for G4M.
# Note that as of R 4.0.0, r(...) raw string constants are an option,
# but we want to support R < 4.0.0

G4M_JOB_TEMPLATE <- c(
  "executable = {bat_path}",
  "arguments = $(job)",
  "universe = vanilla",
  "",
  "nice_user = {ifelse(NICE_USER, 'True', 'False')}",
  "",
  "# Job log, output, and error files",
  "log = {log_dir}/{PREFIX}_$(cluster).$(job).log", # don't use $$() expansion here: Condor creates the log file before it can resolve the expansion
  "output = {log_dir}/{PREFIX}_$(cluster).$(job).out",
  "stream_output = True",
  "error = {log_dir}/{PREFIX}_$(cluster).$(job).err",
  "stream_error = True",
  "", # If a job goes on hold for more than JOB_RELEASE_DELAY seconds, release it up to JOB_RELEASES times
  "periodic_release =  (NumJobStarts <= {JOB_RELEASES}) && (JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > {JOB_RELEASE_DELAY})",
  "",
  "requirements = \\",
  '  ( (Arch =="INTEL")||(Arch =="X86_64") ) && \\',
  '  ( (OpSys == "WINDOWS")||(OpSys == "WINNT61") ) && \\',
  "  ( GLOBIOM =?= True ) && \\",
  "  ( ( TARGET.Machine == \"{str_c(hostdoms, collapse='\" ) || ( TARGET.Machine == \"')}\") )",
  "request_memory = {REQUEST_MEMORY}",
  "request_cpus = {REQUEST_CPUS}", # Number of "CPUs" (hardware threads) to reserve for each job
  "request_disk = {request_disk}",
  "",
  '+IIASAGroup = "ESM"', # Identifies you as part of the group allowed to use ESM cluster
  "run_as_owner = {ifelse(RUN_AS_OWNER, 'True', 'False')}",
  "",
  "should_transfer_files = YES",
  "when_to_transfer_output = ON_EXIT",
  "transfer_output_files = out",
  'transfer_output_remaps = \"out={OUTPUT_DIR}\"',
  "",
  "notification = {NOTIFICATION}",
  '{ifelse(is.null(EMAIL_ADDRESS), "", str_glue("notify_user = {EMAIL_ADDRESS}"))}',
  "",
  "queue job in ({str_c(JOBS,collapse=',')})"
)

#' Run G4M
#'
#' Run G4M by submitting jobs for parallel execution on an HTCondor cluster.
#'
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
run_g4m <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  # Provide and output directory relative to WD_G4M
  if (baseline) {
    output_dir <- path("out", str_glue("{PROJECT}_{DATE_LABEL}\\\\baseline"))
    output_dir_aux <- path(CD, WD_G4M, "out", str_glue("{PROJECT}_{DATE_LABEL}"),"baseline")
    if (!dir_exists(output_dir_aux)) dir_create(output_dir_aux)
  } else {
    output_dir <- path("out", str_glue("{PROJECT}_{DATE_LABEL}"))
  }

  # Get downscaling mapping
  downs_map <-  get_mapping() %>% dplyr::select(-ScenNr) %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>% unique()

  # Save config files
  lab <- str_glue("{PROJECT}_{DATE_LABEL}")
  config <- list(lab,baseline,G4M_EXE,BASE_SCEN1,BASE_SCEN2,BASE_SCEN3)
  save(config, file=path(CD,WD_G4M,"Data","Default","config.RData"))
  save(downs_map, file=path(CD,WD_G4M,"Data","Default","scenario_map.RData"))

  # Determine files for bundle
  default_file_list <- dir_ls(path(CD, WD_G4M, "Data", "Default"))
  glob_file_list <- dir_ls(path(CD, WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}")))
#  base_file_list <- c(default_file_list, glob_file_list)
  base_file_list <- c(default_file_list)
  if (baseline) {
    seed_files <- c(base_file_list, path(CD,WD_G4M,str_glue("{G4M_EXE}")),path(CD, WD_G4M,"g4m_run.R"))
    seed_files <- seed_files[which(!str_detect(seed_files, ".gdx"))]
    output_folder <- str_glue("out/{PROJECT}_{DATE_LABEL}/baseline")
    if (!dir_exists(output_folder)) dir_create(output_folder)

  } else {
    bau_file_list <- str_glue(path(CD, WD_G4M, "out", str_glue("{PROJECT}_{DATE_LABEL}"), "baseline"),"/*.bin")
    seed_files <- c(base_file_list, bau_file_list, str_glue("{G4M_EXE}"),path(CD,WD_G4M,"g4m_run.R"))
    output_folder <- str_glue("out/{PROJECT}_{DATE_LABEL}")
    if (!dir_exists(output_folder)) dir_create(output_folder)
  }

  seed_files <- str_replace_all(seed_files,"/","\\\\")


  # Check if input data is empty
  downs_input <- file_size(path(CD, WD_G4M, "Data","GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}_1.csv")))
  if (downs_input/1024 < 10) stop("Input gdx file might be empty - check reporting script")
  glob_input <- file_size(path(CD, WD_G4M, "Data", "GLOBIOM", str_glue("{PROJECT}_{DATE_LABEL}"), str_glue("output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx")))
  if (glob_input/1024 < 5) stop("Input gdx file might be empty - check reporting script")


  if (baseline) {
    scen_4_g4m <- get_mapping() %>% dplyr::select(-ScenNr) %>%
      filter(SCEN1==BASE_SCEN1 & SCEN2==BASE_SCEN2 & SCEN3==BASE_SCEN3) %>%
      unique() %>% droplevels()
    scen_4_g4m <- scen_4_g4m$ScenLoop
  } else {
    scen_4_g4m <- SCENARIOS_FOR_G4M
  }

  files_include <- str_glue(path(CD,WD_G4M,"Data","GLOBIOM","{PROJECT}_{DATE_LABEL}/*.*"))

  if (length(scen_4_g4m)==0) stop("Scenario(s) not found - check that the G4M baseline is included in the solved scenarios")

  # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'JOBS = c({str_c(scen_4_g4m, collapse=",")})',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 3000',
    'REQUEST_DISK = 1400000',
    'REQUEST_CPUS = 1',
    'JOB_RELEASES = 3',
    'JOB_RELEASE_DELAY = 120',
    'LAUNCHER = "Rscript"',
    'SCRIPT = "{G4M_SUBMISSION_SCRIPT}"',
    'ARGUMENTS = "%1"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'BUNDLE_INCLUDE_FILES =',
    '{seed_files}',
    'BUNDLE_INCLUDE = ',
    '"{files_include}"',
    'JOB_TEMPLATE = ',
    '{G4M_JOB_TEMPLATE}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_OUTPUT = FALSE',
    'OUTPUT_DIR = "{output_folder}"',
    'OUTPUT_FILE = ""'
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
  if (baseline) {save_environment("4")} else {save_environment("5a")}
}

#' Run final post-processing
#'
#' Reads, edits and executes the GLOBIOM 8_merged_output_tmp.gms script to generate
#' reports for IAMC.
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
run_final_postproc_limpopo <- function(cluster_nr_globiom) {

  # Output file directory
  out_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"))
  if(!dir_exists(out_dir)) dir_create(out_dir)
  if (str_detect(CD,"H:")) {gdx_submit <- ""} else {gdx_submit <- str_glue("GDX_OUTPUT_DIR_SUBMIT = \"{out_dir}\"")}

  # Create a tmp copy of the merge output file with a tmp $include
  tempString <- read_lines(path(WD_GLOBIOM, "Model", str_glue("{GLOBIOM_POSTPROC_FILE}")))
  g4m_postproc_file <- string_replace(tempString[which(str_detect(tempString,"8c_[:print:]+.gms"))],c("\\$include\\s+"),"") %>% string_replace(".gms","")
  globiom_postproc_file <- string_replace(GLOBIOM_POSTPROC_FILE,".gms","")

  if (!any(str_detect(tempString,str_glue("{g4m_postproc_file}_tmp.gms")))) tempString <- string_replace(tempString,str_glue("\\$include\\s+{g4m_postproc_file}.gms"),str_glue("$include {g4m_postproc_file}_tmp.gms"))
  tempString[which(str_detect(tempString,regex("GDXIN[:print:]+merged")))] <- str_glue("$GDXIN ..%X%output_%project%_%limpopo_nr%_merged.gdx")
  write_lines(tempString, path(WD_GLOBIOM, "Model", str_glue("{globiom_postproc_file}_tmp.gms")))

  # Construct path for feedback file
  path_feedback <- "..%X%"

  # Get G4M scenario list
  scen_map <-  get_mapping() %>% dplyr::select(-ScenNr) %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>% unique()  %>% slice(1)

  length_scen1 <- length(unlist(str_split(scen_map[,1],"_")))
  length_scen2 <- length(unlist(str_split(scen_map[,3],"_")))
  length_scen3 <- length(unlist(str_split(scen_map[,2],"_")))

  # Define G4M scenarios
  scen <- matrix(unlist(str_split(get_g4m_jobs(baseline = FALSE)[-1]," ")),ncol=4,byrow=T)[,3]
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_aux <- str_split(scen,"_")
  scen_aux <- matrix(unlist(scen_aux),ncol = length(scen_aux[[1]]), byrow = TRUE)
  scen_globiom_map <- array(dim=c(length(scen),3),data="")
  for(i in 1:dim(scen_aux)[1]){
    scen_globiom_map[i,1] <- do.call(str_glue, c(as.list(scen_aux[i,1:length_scen1]), .sep = "_"))
    scen_globiom_map[i,2] <- do.call(str_glue, c(as.list(scen_aux[i,(length_scen1 + 1):(length_scen1 + length_scen2)]), .sep = "_"))
    scen_globiom_map[i,3] <- do.call(str_glue, c(as.list(scen_aux[i,c(-1:-(length_scen1 + length_scen2))]), .sep = "_"))
  }


  # Check if scenario name must be treated as string
  special_char <- FALSE
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if (special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  # Define column indices of Macro, Bioen and IEA scenarios
  macro_idx <- which(str_detect(scen_globiom_map[1,],regex("SSP",ignore_case = T)))
  iea_idx <- which(str_detect(scen_globiom_map[1,],regex("RCP",ignore_case = T)))
  bioen_idx <- 6 - macro_idx - iea_idx

  if(special_char) scen_globiom_map[,bioen_idx] <- unlist(lapply(scen_globiom_map[,bioen_idx],function(x) str_glue("\"",x,"\"")))

  # Create scenario mapping string
  for (i in 1:length(scen)) {
    if (i == 1) {
      map_string <-
        str_glue(scen[i],
                 " . ",
                 scen_globiom_map[i, macro_idx],
                 " . ",
                 scen_globiom_map[i, bioen_idx],
                 " . ",
                 scen_globiom_map[i, iea_idx])
    } else {
      map_string <-
        c(
          map_string,
          str_glue(
            scen[i],
            " . ",
            scen_globiom_map[i, macro_idx],
            " . ",
            scen_globiom_map[i, bioen_idx],
            " . ",
            scen_globiom_map[i, iea_idx]
          )
        )
    }
  }

  # Define sets for mapping
  g4m_globiom_map <- c("G4MScen2","/",scen,"/","","G4M_SCEN_MAP(G4MScen2,*,*,*)",
                       "/",map_string,"/",";")

  # Edit mapping set
  tempString <- read_file(path(WD_GLOBIOM, "Model", str_glue("{g4m_postproc_file}.gms")))
  tempString <- string_replace(tempString,regex('G4MScen2[[:print:]*|[\r\n]*]*G4M_SCEN_MAP[[:print:]*|[\r\n]*]*/[\r\n\\s]+;'),
                               str_c(g4m_globiom_map,collapse="\n"))

  # Edit feedback file
  path_for_feedback_file <- str_glue(path_feedback,G4M_FEEDBACK_FILE)

  tempString <- string_replace(tempString,"\\$include\\s+[:print:]*X[:print:]*",
                               str_glue('$include "{path_for_feedback_file}"'))

  # Edit baseline scenario for forest management GHG accounting
  ref_sum_reg <-  str_glue("- G4M_SCENOUTPUT_DATA(REGION,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ScenYear)")
  ref_reg <-  "-\\s*G4M_SCENOUTPUT_DATA[:print:]REGION,[:print:]+,\"em_fm_bm[:print:]+\",ScenYear[:print:]"

  ref_sum_ctry <-  str_glue("- G4M_SCENOUTPUT_DATA_RAW(COUNTRY,\"{BASE_SCEN1}\",\"{BASE_SCEN2}\",\"{BASE_SCEN3}\",\"em_fm_bm_mtco2year\",ALLYEAR)")
  ref_ctry <-  "-\\s*G4M_SCENOUTPUT_DATA_RAW[:print:]COUNTRY,[:print:]+,\"em_fm_bm[:print:]+\",ALLYEAR[:print:]"

  # Check if aggregation is done at country or region level
  country_aggregation <- str_detect(tempString,regex(ref_ctry, ignore_case = T))

  tempString <- string_replace_all(tempString,regex(ifelse(country_aggregation,ref_ctry,ref_reg), ignore_case = T),
                                   ifelse(country_aggregation,ref_sum_ctry,ref_sum_reg))

  write_lines(tempString, path(CD,WD_GLOBIOM,"Model",str_glue("{g4m_postproc_file}_tmp.gms")))

  # Define files to bundle
  glob_file <- path(CD,WD_GLOBIOM,"Model","gdx",str_glue("output_{PROJECT}_{cluster_nr_globiom}_merged.gdx"))
  g4m_file <- path(CD,WD_GLOBIOM,"Model","output","g4m",str_glue("{PROJECT}_{DATE_LABEL}"),G4M_FEEDBACK_FILE)

  # Save edits and run post-processing script in the GLOBIOM Model directory
  prior_wd <- getwd()
  rc <- tryCatch ({

    # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
    config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = 0',
      'HOST_REGEXP = "^limpopo"',
      'REQUEST_MEMORY = 200000',
      'REQUEST_DISK = 4000000',
      'REQUEST_CPUS = 1',
      'GAMS_CURDIR = "Model"',
      'GAMS_FILE_PATH = "{globiom_postproc_file}_tmp.gms"',
      'GAMS_VERSION = "32.2"',
      'GAMS_ARGUMENTS = "//limpopo=yes //limpopo_nr={cluster_nr_globiom} //project={PROJECT} //lab={DATE_LABEL} //rep_g4m=no //rep_iamc_glo=yes //rep_iamc_g4m=yes //g4mfile={G4M_FEEDBACK_FILE} //regionagg={REGIONAL_AG} //nsim=%1"',
      'BUNDLE_INCLUDE = "Model"',
      'BUNDLE_INCLUDE_DIRS = c("include")',
      'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst","**/output/iamc/*.*","**/output/g4m/*.*","**/gdx/*.*") # supports wildcards',
      'BUNDLE_ADDITIONAL_FILES = c("{g4m_file}","{glob_file}")',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_GDX_OUTPUT = TRUE',
      'GDX_OUTPUT_DIR = "output/iamc"',
      '{gdx_submit}',
      'GDX_OUTPUT_FILE = "Output4_IAMC_template_{PROJECT}_{DATE_LABEL}.gdx"'
    )

    config_path <- path(TEMP_DIR, "config_glob.R")

    # Write config file
    current_env <- environment()
    write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
    rm(config_template, current_env)

    # Submit GLOBIOM scenarios and wait for run completion
    rc <- tryCatch ({
      setwd(WD_GLOBIOM)
      system(str_glue("Rscript --vanilla {CD}/Condor_run_R/Condor_run.R {config_path}"))
    },
    finally = {
      setwd(prior_wd)
    })
    if (rc != 0) stop("Condor run failed!")

    # Transfer files
    if (gdx_submit == "") transfer_outputs()

  },
  finally = {
    setwd(prior_wd)
  })
}




#' Run downscaling post-processing
#'
#' Downscales G4M outputs back to GLOBIOM SimUID
#'
run_downscaling_postproc <- function() {

  # Get G4M scenario list
  scenario_mapping <- get_mapping() %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M)

  # Define downscaling scenarios for limpopo run
  scen_string <- "c("
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[i])]
    if (i==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
      scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
  }
  scen_string <- str_glue(scen_string,")")

  # Save scenario grid and additional configuration to input data
  saveRDS(scenario_mapping,path(WD_DOWNSCALING,"input","scenario_grid.RData"))
  saveRDS(c(PROJECT,DATE_LABEL,cluster_nr_downscaling),path(WD_DOWNSCALING,"input","config.RData"))

  # Define files to bundle
  downscaling_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp = str_glue("output_{PROJECT}_{cluster_nr_downscaling}.*.RData")) %>%
  str_replace_all("/","\\\\") %>% sort()

  g4m_files <- dir_ls(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}")),
                      regexep=str_glue(.Platform$file.sep,"area"))
  idx <- which(!is.na(str_match(g4m_files,str_glue("area_harv"))))
  g4m_files <- g4m_files[idx]
  seed_files <- c(downscaling_files,g4m_files)
  seed_files <- str_replace_all(seed_files,"/","\\\\")

  if (!dir_exists(path(CD,WD_DOWNSCALING,"postproc"))) dir_create(path(CD,WD_DOWNSCALING,"postproc"))
  file_copy(g4m_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
  file_copy(downscaling_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
  include <- str_glue(c("**/gdx/output_{PROJECT}_{DATE_LABEL}_{cluster_nr_downscaling}.*.RData"))

    config_template <- c(
      'EXPERIMENT = "{PROJECT}"',
      'JOBS = {scen_string}',
      'HOST_REGEXP = "^limpopo"',
      'REQUEST_MEMORY = 5000',
      'BUNDLE_EXCLUDE_FILES = c("**/gdx/*.*")',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 3',
      'JOB_RELEASE_DELAY = 120',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "run_downscaling_postproc.R"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_OUTPUT = TRUE',
      'OUTPUT_DIR = "gdx"',
      'OUTPUT_FILE = "g4m_simu_out.RData"'
    )

  config_path <- path(TEMP_DIR, "config_postproc.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_DOWNSCALING)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R//Condor_run_basic.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })
  if (rc != 0) stop("Condor run failed!")

  # remove temp files
  clear_g4mtosimu_files()
}



#' Run downscaling post-processing
#'
#' Downscales G4M outputs back to GLOBIOM SimUID in batches
#' for each scenario, to avoid disk space bottlenecks on limpopo
#'
run_downscaling_postproc_split <- function() {

  # Get G4M scenario list
  scenario_mapping <- get_mapping() %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M)

  # Save scenario grid and additional configuration to input data
  saveRDS(scenario_mapping,path(WD_DOWNSCALING,"input","scenario_grid.RData"))
  saveRDS(c(PROJECT,DATE_LABEL,cluster_nr_downscaling),path(WD_DOWNSCALING,"input","config.RData"))

  # Define downscaling scenarios for limpopo run
  for (i in 1: length(SCENARIOS_FOR_DOWNSCALING)){
    scen_string <- ""
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[i])]
    scen_string <- str_glue("c(",scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)),")")

  # Define files to bundle
  downscaling_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("output_{PROJECT}_{cluster_nr_downscaling}.",
                                                                    "*",".RData"))  %>% sort()
  idx <-  which(!is.na(str_match(downscaling_files,sprintf("%06d",scenarios_idx))))
  #  str_replace_all("/","\\\\") %>% sort()

  downscaling_files <- downscaling_files[idx]

  g4m_files <- dir_ls(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}")),
                      regexep=str_glue(.Platform$file.sep,"area"))
  s_nr <- which(scenario_mapping$ScenNr==scenarios_idx[1])
  idx <- which(!is.na(str_match(g4m_files,str_glue("area_harv[:print:]+",paste(scenario_mapping$SCEN1[s_nr]),
                                                   "_",paste(scenario_mapping$SCEN3[s_nr]),"_",
                                                   paste(scenario_mapping$SCEN2[s_nr])))))
  g4m_files <- g4m_files[idx]
  seed_files <- c(downscaling_files,g4m_files)
  #seed_files <- str_replace_all(seed_files,"/","\\\\")

  if (!dir_exists(path(CD,WD_DOWNSCALING,"postproc"))) dir_create(path(CD,WD_DOWNSCALING,"postproc"))
  file_copy(g4m_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
  file_copy(downscaling_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
  #include <- str_glue(c("**/gdx/output_{PROJECT}_{DATE_LABEL}_{cluster_nr_downscaling}.*.RData"))

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'JOBS = {scen_string}',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 5000',
    'BUNDLE_EXCLUDE_FILES = c("**/gdx/*.*","**/input/*.gdx")',
    'BUNDLE_EXCLUDE_DIRS = c("output", "prior_module", "source","t")',
    'REQUEST_CPUS = 1',
    'REQUEST_DISK = 10000000',
    'JOB_RELEASES = 3',
    'JOB_RELEASE_DELAY = 120',
    'LAUNCHER = "Rscript"',
    'SCRIPT = "run_downscaling_postproc.R"',
    'ARGUMENTS = "%1"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_OUTPUT = TRUE',
    'OUTPUT_DIR = "gdx"',
    'OUTPUT_FILE = "g4m_simu_out.RData"'
  )

  config_path <- path(TEMP_DIR, "config_postproc.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_DOWNSCALING)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R//Condor_run_basic.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })

  unlink(path(CD,WD_DOWNSCALING,"postproc","*.*"),recursive = TRUE)

  if (rc != 0) stop("Condor run failed!")
  }

}




#' Run merge and transfer
#'
#' Sends data compilation from Downscaling to G4M to limpopo
#' @param cluster_nr_downscaling Cluster sequence number of the downscaling HTCondor submission
run_merge_and_transfer <- function(cluster_nr_downscaling) {

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config <- list()

  # Save config file
  config[[1]] <- get_mapping()
  config[[2]] <- DOWNSCALING_TYPE
  config[[3]] <- cluster_nr_downscaling
  config[[4]] <- PROJECT
  config[[5]] <- DATE_LABEL

  saveRDS(config,path(CD,WD_DOWNSCALING,"config.RData"))

  # Get downscaled files (!transfer is faster than using BUNDLE_ADDITIONAL_FILES)
  if (!DOWNSCALING_TYPE == "downscalr") {
    include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("downscaled_{PROJECT}_{cluster_nr_downscaling}.*.*"))
  } else {
    include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("output_{PROJECT}_{cluster_nr_downscaling}.*.*"))
  }

  exclude_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx")) %>%
                    str_extract(str_glue("_[0-9]+.")) %>% unique() %>% na.omit()

  # File transfer is faster than BUNDLE_ADDITIONAL_FILES
  if(!dir_exists(path(CD,WD_DOWNSCALING,"g4m_merge"))) dir_create(path(CD,WD_DOWNSCALING,"g4m_merge"))
  file_copy(include_files,path(CD,WD_DOWNSCALING,"g4m_merge"),overwrite = T)

  config_template <- c(
    'EXPERIMENT = "{PROJECT}"',
    'JOBS = {SCENARIOS_FOR_DOWNSCALING}',
    'HOST_REGEXP = "^limpopo"',
    'REQUEST_MEMORY = 5000',
    'BUNDLE_EXCLUDE_DIRS = c("output", "prior_module", "source","t","postproc","renv","gdx")',
    'BUNDLE_EXCLUDE_FILES = c(".Rprofile","renv.lock","input/*.gdx")',
#    'BUNDLE_ADDITIONAL_FILES = ',
#    '{include_files}',
    'REQUEST_CPUS = 1',
    'REQUEST_DISK = 20000000',
    'JOB_RELEASES = 3',
    'JOB_RELEASE_DELAY = 120',
    'LAUNCHER = "Rscript"',
    'SCRIPT = "compile_LC_data.R"',
    'ARGUMENTS = "%1"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'RETAIN_BUNDLE = FALSE',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_OUTPUT = TRUE',
    'OUTPUT_DIR = "gdx"',
    'OUTPUT_FILE = "GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}.csv"'
  )

  config_path <- path(TEMP_DIR, "config_merge.R")

  # Write config file
  current_env <- environment()
  write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
  rm(config_template, current_env)

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_DOWNSCALING)
    system(str_glue("Rscript --vanilla {CD}/Condor_run_R//Condor_run_basic.R {config_path}"))
  },
  finally = {
    setwd(prior_wd)
  })

  # Clean temporary folder
  unlink(path(CD,WD_DOWNSCALING,"g4m_merge","*.*"),recursive = TRUE)

  cluster_nr <- readr::parse_number(read_file(cluster_number_log))

  # rename and transfer to g4m
  g4m_dir <- path(CD,str_glue("{WD_G4M}"),"Data","GLOBIOM",str_glue("{PROJECT}_{DATE_LABEL}"))
  if (!dir_exists(g4m_dir)) dir_create(g4m_dir)

  for (k in SCENARIOS_FOR_DOWNSCALING){
    limpopo_f <- str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}_{PROJECT}_{cluster_nr}.",sprintf("%06d",k),".csv")
    g4m_f <- str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}_",k+1,".csv")

    file_move(path(CD,WD_DOWNSCALING,"gdx",limpopo_f),
              path(CD,str_glue("{WD_G4M}"),"Data","GLOBIOM",str_glue("{PROJECT}_{DATE_LABEL}"),g4m_f))
  }

}
