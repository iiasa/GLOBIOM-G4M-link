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
    'REQUIREMENTS = c("GLOBIOM")',
    'REQUEST_MEMORY = 20000',
    'REQUEST_DISK = 2200000',
    'REQUEST_CPUS = 1',
    'GAMS_CURDIR = "Model"',
    'GAMS_FILE_PATH = "{GLOBIOM_SCEN_FILE}"',
    'GAMS_VERSION = "{GAMS_VERSION}"',
    'GAMS_ARGUMENTS = "{GLOBIOM_GAMS_ARGS}"',
    'BUNDLE_INCLUDE = "Model"',
    'BUNDLE_INCLUDE_DIRS = c("include")',
    'BUNDLE_EXCLUDE_DIRS = c("Model/t","Model/output",".git", ".svn", "225*", "doc")',
    'BUNDLE_EXCLUDE_FILES = c("**/*.~*","**/~*.*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst","**/output/g4m/*.*","**/output/iamc/*.*","**/output/g4m/*.*","**/gdx/*.*") # supports wildcards',
    'BUNDLE_ADDITIONAL_FILES = c()',
    'RESTART_FILE_PATH = "t/{GLOBIOM_RESTART_FILE}"',
    'RUN_AS_OWNER = {RUN_AS_OWNER}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_GDX_OUTPUT = TRUE',
    'GDX_OUTPUT_DIR = "gdx"',
    'GDX_OUTPUT_FILE = "output.gdx"',
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
    system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run.R" "{config_path}"'))
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
      'REQUIREMENTS = c("GAMS")',
      'REQUEST_MEMORY = 2500',
      'REQUEST_DISK = 1400000',
      'REQUEST_CPUS = 1',
      'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
      'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
      'GAMS_VERSION = "{GAMS_VERSION}"',
      'GAMS_ARGUMENTS = "//project={PROJECT} //lab={DATE_LABEL} //gdx_path=gdx/downscaled.gdx //nsim=%1"',
      'BUNDLE_INCLUDE_DIRS = c("include")',
      'JOB_OVERRIDES = list("periodic_release" = "periodic_remove = (JobStatus == 5)")',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_GDX_OUTPUT = TRUE',
      'GDX_OUTPUT_DIR = "gdx"',
      'GDX_OUTPUT_FILE = "downscaled.gdx"',
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

      system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run.R" "{config_path}"'))
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
        'REQUIREMENTS = c("GAMS"),',
        'HOST_REGEXP = "^limpopo"',
        'REQUEST_MEMORY = 2500',
        'REQUEST_DISK = 1400000',
        'REQUEST_CPUS = 1',
        'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
        'GAMS_FILE_PATH = "{DOWNSCALING_SCRIPT}"',
        'GAMS_VERSION = "{GAMS_VERSION}"',
        'GAMS_ARGUMENTS = "//project={PROJECT} //lab={DATE_LABEL} //gdx_path=gdx/downscaled.gdx //nsim=%1"',
        'BUNDLE_INCLUDE_DIRS = c("include")',
        'JOB_OVERRIDES = list("periodic_release" = "periodic_remove = (JobStatus == 5)")',
        'RUN_AS_OWNER = {RUN_AS_OWNER}',
        'WAIT_FOR_RUN_COMPLETION = TRUE',
        'CLEAR_LINES = FALSE',
        'GET_GDX_OUTPUT = TRUE',
        'GDX_OUTPUT_DIR = "gdx"',
        'GDX_OUTPUT_FILE = "downscaled.gdx"',
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

        system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run.R" "{config_path}"'))
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
        regexp = str_glue("downscaled_{current_cluster}.*.gdx")
      ),
      function(x) {
        scen_nr <- str_sub(x, -10, -5)

        file_move(x, path(
          CD,
          WD_DOWNSCALING,
          "gdx",
          str_glue("downscaled_{cluster}.{scen_nr}.gdx")
        ))

      })

      # Set cluster number to original value
      write_file(as.character(cluster),cluster_number_log)

    }

  } else {

    # Statistical downscaling variant

    # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
    config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = c({scen_string})',
      'REQUIREMENTS = c("R")',
      'REQUEST_MEMORY = 7500',
      'REQUEST_DISK = 1500000',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 1',
      'JOB_RELEASE_DELAY = 120',
      'BUNDLE_EXCLUDE_FILES = "**/gdx/*.*"',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "{DOWNSCALR_SCRIPT}"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'BUNDLE_INCLUDE = "*"',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
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
    downscaling_pars[[6]] <- REGION_RESOLUTION
    downscaling_pars[[7]] <- THERE_ARE_BTC_Scenarios
    downscaling_pars[[8]] <- SCENARIOS_BTC

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

      system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
    },
    finally = {
      setwd(prior_wd)
    })

  }

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

  # Prepare historical input files for G4M (choose the correct ones for current REGION_RESOLUTION, i.e. REGION37 or REGION59)
  for(G4MInput in c("LandRent","SupplyWood","Wood_price")){
    G4MInput_filename <- paste0(path(CD,WD_G4M,"Data","Default"),"/","GLOBIOM2G4M_output_",G4MInput,"_beis_08022022_2000_2020.csv")
    G4MInput_filename_curREGION <- paste0(path(CD,WD_G4M,"Data","Default"),"/","GLOBIOM2G4M_output_",G4MInput,"_beis_08022022_2000_2020_",paste0("REGION",REGION_RESOLUTION),".csv")

  if(file.exists(G4MInput_filename)){file.remove(G4MInput_filename)}
  file.copy(from=G4MInput_filename_curREGION,to=G4MInput_filename)
  }

  # Prepare historical input files for G4M (choose the correct ones for current REGION_RESOLUTION, i.e. REGION37 or REGION59)
    G4MIni_filename <- paste0(path(CD,WD_G4M,"Data","Default"),"/","settings_glob_cell_nas_v10_ukbeis_3_allScen.ini")
    GG4MIni_filename_curREGION <- paste0(path(CD,WD_G4M,"Data","Default"),"/","settings_glob_cell_nas_v10_ukbeis_3_allScen_",paste0("REGION",REGION_RESOLUTION),".ini")
    if(file.exists(G4MIni_filename)){file.remove(G4MIni_filename)}
    file.copy(from=GG4MIni_filename_curREGION,to=G4MIni_filename)
  
  
  
  # Get downscaling mapping
  downs_map <-  get_mapping() %>% dplyr::select(-ScenNr) %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>% 
    dplyr::select(-RegionName) %>% unique()

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
    if (!dir_exists(path(CD,WD_G4M,output_folder))) dir_create(path(CD,WD_G4M,output_folder))

  } else {
    bau_file_list <- str_glue(path(CD, WD_G4M, "out", str_glue("{PROJECT}_{DATE_LABEL}"), "baseline"),"/*.bin")
    seed_files <- c(base_file_list, bau_file_list, str_glue("{G4M_EXE}"),path(CD,WD_G4M,"g4m_run.R"))
    output_folder <- str_glue("out/{PROJECT}_{DATE_LABEL}")
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
      dplyr::select(-RegionName) %>%
      unique() %>% droplevels()
    scen_4_g4m <- scen_4_g4m$ScenLoop
  } else {
    scen_4_g4m <- SCENARIOS_FOR_G4M
  }


  files_include <- str_glue(path(CD,WD_G4M,"Data","GLOBIOM","{PROJECT}_{DATE_LABEL}/*.*"))

  if (length(scen_4_g4m)==0) stop("Scenario(s) not found - check that the G4M baseline is included in the solved scenarios")

  # Define configuration template as per https://github.com/iiasa/Condor_run_R/blob/master/configuring.md
  config_template <- c(
    'LABEL = "{PROJECT}"',
    'JOBS = c({str_c(scen_4_g4m, collapse=",")})',
    'REQUIREMENTS = c("R")',
    # 'REQUEST_MEMORY = 3000',
    'REQUEST_MEMORY = 3900',
    'REQUEST_DISK = 1400000',
    'REQUEST_CPUS = 1',
    'JOB_RELEASES = 1',
    'JOB_RELEASE_DELAY = 120',
    'HOST_REGEXP = "^limpopo[678]"', # suggested hosts from the cluster for running G4M tasks
    'LAUNCHER = "Rscript"',
    'SCRIPT = "{G4M_SUBMISSION_SCRIPT}"',
    'ARGUMENTS = "%1"',
    'DATE_LABEL = "{DATE_LABEL}"',
    'BUNDLE_INCLUDE_FILES =',
    '{seed_files}',
    'BUNDLE_INCLUDE = ',
    '"{files_include}"',
    'JOB_OVERRIDES = list("transfer_output_files" = "transfer_output_files = out", "transfer_output_remaps" = \'transfer_output_remaps = "out = {output_folder}"\')',
    'RUN_AS_OWNER = {RUN_AS_OWNER}',
    'WAIT_FOR_RUN_COMPLETION = TRUE',
    'CLEAR_LINES = FALSE',
    'GET_OUTPUT = TRUE',
    'OUTPUT_DIR = "out"',
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
    if(!dir_exists("out")) dir_create("out")
    system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
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
    filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>% unique() %>% droplevels()

  length_scen1 <- scen_map[[1]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))
  length_scen2 <- scen_map[[3]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))
  length_scen3 <- scen_map[[2]] %>% sapply(FUN=function(x) length(unlist(str_split(x,"_"))))

  # Define G4M scenarios
  scen <- matrix(unlist(str_split(get_g4m_jobs(baseline = FALSE)[-1]," ")),ncol=4,byrow=T)[,3]
  scen <- scen[which(scen != "")]

  # Split G4M scenarios into GLOBIOM dimensions
  scen_globiom_map <- array(dim=c(length(scen),3),data="")

  for(i in 1:length(scen)){
    scen_aux <- scen[i] %>% str_split("_") %>% unlist()
    scen_globiom_map[i,1] <- do.call(str_glue, c(as.list(scen_aux[1:length_scen1[i]]), .sep = "_"))
    scen_globiom_map[i,2] <- do.call(str_glue, c(as.list(scen_aux[(length_scen1[i] + 1):(length_scen1[i] + length_scen2[i])]), .sep = "_"))
    scen_globiom_map[i,3] <- do.call(str_glue, c(as.list(scen_aux[c(-1:-(length_scen1[i] + length_scen2[i]))]), .sep = "_"))
  }

  # Check if scenario name must be treated as string
  special_char <- FALSE
  if (any(str_detect(scen,"%"))) special_char <- TRUE
  if (special_char)  scen <- unlist(lapply(scen,function(x) str_glue("\"",x,"\"")))

  if(special_char) scen_globiom_map[,bioen_idx] <- unlist(lapply(scen_globiom_map[,bioen_idx],function(x) str_glue("\"",x,"\"")))

  # Create scenario mapping string
  for (i in 1:length(scen)) {
    if (i == 1) {
      map_string <-
        str_glue(scen[i],
                 " . ",
                 scen_globiom_map[i, 1],
                 " . ",
                 scen_globiom_map[i, 3],
                 " . ",
                 scen_globiom_map[i, 2])
    } else {
      map_string <-
        c(
          map_string,
          str_glue(
            scen[i],
            " . ",
            scen_globiom_map[i, 1],
            " . ",
            scen_globiom_map[i, 3],
            " . ",
            scen_globiom_map[i, 2]
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
      'REQUIREMENTS = c("GLOBIOM")',
      'REQUEST_MEMORY = 200000',
      'REQUEST_DISK = 4000000',
      'REQUEST_CPUS = 1',
      'GAMS_CURDIR = "Model"',
      'GAMS_FILE_PATH = "{globiom_postproc_file}_tmp.gms"',
      'GAMS_VERSION = "{GAMS_VERSION}"',
      'GAMS_ARGUMENTS = "//limpopo=yes //limpopo_nr={cluster_nr_globiom} //project={PROJECT} //lab={DATE_LABEL} //rep_g4m=no //rep_iamc_glo=yes //rep_iamc_g4m=yes //g4mfile={G4M_FEEDBACK_FILE} //regionagg={REGIONAL_AG} //nsim=%1"',
      'BUNDLE_INCLUDE = "Model"',
      'BUNDLE_INCLUDE_DIRS = c("include")',
      'BUNDLE_EXCLUDE_FILES = c("**/*.~*", "**/*.log", "**/*.log~*", "**/*.lxi", "**/*.lst","**/output/iamc/*.*","**/output/g4m/*.*","**/gdx/*.*") # supports wildcards',
      'BUNDLE_ADDITIONAL_FILES = c("{g4m_file}","{glob_file}")',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_GDX_OUTPUT = TRUE',
      'GDX_OUTPUT_DIR = "output/iamc"',
      '{gdx_submit}',
      'GDX_OUTPUT_FILE = "Output4_IAMC_template_{PROJECT}_{cluster_nr_globiom}_{REGIONAL_AG}.gdx"'
    )

    config_path <- path(TEMP_DIR, "config_glob.R")

    # Write config file
    current_env <- environment()
    write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
    rm(config_template, current_env)

    # Submit GLOBIOM scenarios and wait for run completion
    rc <- tryCatch ({
      setwd(WD_GLOBIOM)
      system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run.R" "{config_path}"'))
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
  downscaling_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp = str_glue("output_{cluster_nr_downscaling}.*.RData")) %>%
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
  include <- str_glue(c("**/gdx/output_{cluster_nr_downscaling}.*.RData"))

    config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = {scen_string}',
      'REQUIREMENTS = c("R")',
      'REQUEST_MEMORY = 5000',
      'BUNDLE_EXCLUDE_FILES = c("**/gdx/*.*","**/input/*.gdx",".Rprofile")',
      'BUNDLE_EXCLUDE_DIRS = c("output", "prior_module", "source","t","renv")',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 3',
      'JOB_RELEASE_DELAY = 120',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "run_downscaling_postproc.R"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
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
    system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
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

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  cluster_nr_ids <- c()

  # Get G4M scenario list
  scenario_mapping <- get_mapping() %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M)

  # Save scenario grid and additional configuration to input data
  saveRDS(scenario_mapping,path(WD_DOWNSCALING,"input","scenario_grid.RData"))
  saveRDS(c(PROJECT,DATE_LABEL,cluster_nr_downscaling),path(WD_DOWNSCALING,"input","config.RData"))

  # Create submission blocks of 10 scenarios
  scen_blocks <- divide(SCENARIOS_FOR_G4M,ceiling(length(SCENARIOS_FOR_G4M)/15))

  # Define downscaling scenarios for limpopo run
  for (i in 1: length(scen_blocks)){
    #scen_string <- ""
    scenarios_idx_all <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% scen_blocks[[i]])]

    scen_string <- "c("
    for (j in 1:length(scen_blocks[[i]])){
      scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% scen_blocks[[i]][j])]
      if (j==1) {scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)))} else {
        scen_string <- str_glue(scen_string,",",str_glue(min(scenarios_idx),":",max(scenarios_idx)))}
    }
    scen_string <- str_glue(scen_string,")")

    # Define files to bundle
    downscaling_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("output_{cluster_nr_downscaling}.",
                                                                              "*",".RData"))  %>% sort() %>%
      match_str(sprintf("%06d",scenarios_idx_all))
    #idx <-  which(!is.na(str_match(downscaling_files,sprintf("%06d",scenarios_idx_all))))
    #  str_replace_all("/","\\\\") %>% sort()

    #downscaling_files <- downscaling_files[idx]


    g4m_files <- dir_ls(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}")),
                        regexp ="\\.csv$") %>% str_subset("area")

    add_files <- c(g4m_files,downscaling_files)

    #if (!dir_exists(path(CD,WD_DOWNSCALING,"postproc"))) dir_create(path(CD,WD_DOWNSCALING,"postproc"))
    #file_copy(g4m_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
    #file_copy(downscaling_files,path(CD,WD_DOWNSCALING,"postproc"),overwrite = T)
    #include <- str_glue(c("**/gdx/output_{PROJECT}_{DATE_LABEL}_{cluster_nr_downscaling}.*.RData"))

    config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = {scen_string}',
      'REQUIREMENTS = c("R")',
      'REQUEST_MEMORY = 5000',
      'BUNDLE_EXCLUDE_FILES = c("**/gdx/*.*","**/input/*.gdx",".Rprofile")',
      'BUNDLE_EXCLUDE_DIRS = c("output", "prior_module", "source","t","renv")',
      'BUNDLE_ADDITIONAL_FILES = ',
      '{add_files}',
      'REQUEST_CPUS = 1',
      'REQUEST_DISK = 10000000',
      'JOB_RELEASES = 3',
      'JOB_RELEASE_DELAY = 120',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "run_downscaling_postproc.R"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_OUTPUT = TRUE',
      'OUTPUT_DIR = "gdx"',
      'OUTPUT_FILE = "g4m_simu_out.RData"',
      'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
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

    #unlink(path(CD,WD_DOWNSCALING,"postproc","*.*"),recursive = TRUE)

    if (rc != 0) stop("Condor run failed!")

    # Save cluster number
    cluster <- readr::parse_number(read_file(cluster_number_log))
    cluster_nr_ids <- c(cluster_nr_ids,cluster)

  }

  return(cluster_nr_ids)

}




#' Run merge and transfer
#'
#' Sends data compilation from Downscaling to G4M to limpopo
#' @param cluster_nr_downscaling Cluster sequence number of the downscaling HTCondor submission
run_merge_and_transfer <- function(cluster_nr_downscaling) {

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config <- list()

  # Define scenario count
  scen_cnt <- tibble(scenid=1:length(SCENARIOS_FOR_DOWNSCALING),scencnt=as.integer(SCENARIOS_FOR_DOWNSCALING))

  # Save config file
  config[[1]] <- get_mapping()
  config[[2]] <- DOWNSCALING_TYPE
  config[[3]] <- cluster_nr_downscaling
  config[[4]] <- PROJECT
  config[[5]] <- DATE_LABEL
  config[[6]] <- scen_cnt

  saveRDS(config,path(CD,WD_DOWNSCALING,"config.RData"))

  # Get downscaled files (!transfer is faster than using BUNDLE_ADDITIONAL_FILES)
  if (!DOWNSCALING_TYPE == "downscalr") {
    include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("downscaled_{cluster_nr_downscaling}.*.*"))
  } else {
    include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("output_{cluster_nr_downscaling}.*.*"))
  }

  exclude_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx")) %>%
                    str_extract(str_glue("_[0-9]+.")) %>% unique() %>% na.omit()

  # File transfer is faster than BUNDLE_ADDITIONAL_FILES
  if(!dir_exists(path(CD,WD_DOWNSCALING,"g4m_merge"))) dir_create(path(CD,WD_DOWNSCALING,"g4m_merge"))
  file_copy(include_files,path(CD,WD_DOWNSCALING,"g4m_merge"),overwrite = T)

  config_template <- c(
    'LABEL = "{PROJECT}"',
    'JOBS = c({str_c(SCENARIOS_FOR_DOWNSCALING, collapse=",")})',
    'REQUIREMENTS = c("R")',
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
    'RUN_AS_OWNER = {RUN_AS_OWNER}',
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
    system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
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


#' Run biodiversity indices computation
#'
#' Uses the land cover at SimU level to compute the PREDICTS - BII
#' and cSAR biodiversity indices
#'
#' @param cluster_nr_downscaling_postproc Cluster sequence number of the downscaling post-processing HTCondor submission
run_biodiversity <- function(cluster_nr_downscaling) {

  # Create output folder
  if (!dir_exists(path(CD,WD_BIODIVERSITY,"Output"))) dir_create(path(CD,WD_BIODIVERSITY,"Output"))

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  # Define scenarion mapping
  scenario_mapping <- get_mapping() %>%
    filter(ScenLoop %in% SCENARIOS_FOR_BIODIVERSITY)

  # Define out id for merging gdx
  # out_id <- format(Sys.time(), "%d %X %Y") %>% str_remove_all(":") %>% str_remove_all(" ")  ## out_id identifier, 1st Alternative - use system time
  out_id <- paste0(PROJECT,"_",DATE_LABEL) ## out_id identifier, 2ND Alternative - use {PROJECT} and {DATE_LABEL}

  # Split scenarios into submission blocks of 40 scenarios
  scen_blocks <- ifelse(length(SCENARIOS_FOR_BIODIVERSITY)<10,
                        list(SCENARIOS_FOR_BIODIVERSITY),
                        divide(SCENARIOS_FOR_BIODIVERSITY,ceiling(length(SCENARIOS_FOR_BIODIVERSITY)/40)))

  # Define downscaling scenarios for biodiversity run
  for (i in 1: length(scen_blocks)){

    block_scen <- NA
    scen_string <- "c("
    for(j in 1:length(scen_blocks[[i]])){
      scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% scen_blocks[[i]][j])]
      block_scen <- c(block_scen,scenarios_idx)
      if (j==length(scen_blocks[[i]])){
        scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)),")")
      } else{
        scen_string <- str_glue(scen_string,str_glue(min(scenarios_idx),":",max(scenarios_idx)),",")
      }
    }


    # Define files to bundle
    downscaling_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),
                                regexp=str_glue("output_{cluster_nr_downscaling}.",
                                                                              "*",".RData"))  %>% sort() %>%
      match_str(sprintf("%06d",block_scen[-1]))

    scens <- scenario_mapping %>% filter(ScenLoop %in% scen_blocks[[i]]) %>%
      mutate(id = str_c("area[:print:]*",SCEN1,"_",SCEN3,"_",SCEN2)) %>% dplyr::select(id) %>% unique()

    g4m_files <- dir_ls(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}")),
                        regexp ="\\.csv$") %>% match_str(scens$id)

    add_files <- c(g4m_files,downscaling_files)

    # Create config for biodiversity runs
    config <- list(PROJECT,DATE_LABEL,scenario_mapping,cluster_nr_downscaling, COMPUTE_BII, COMPUTE_cSAR)

    saveRDS(config,path(CD,WD_BIODIVERSITY,"Input","config.RData"))

     config_template <- c(
      'LABEL = "{PROJECT}"',
      'JOBS = {scen_string}',
      'HOST_REGEXP = "^limpopo"',
      'REQUIREMENTS = c("R")',
      'REQUEST_MEMORY = 10000',
      'BUNDLE_EXCLUDE_FILES = c("**/Output/*.*","Output/*.*")',
      'BUNDLE_ADDITIONAL_FILES = ',
      '{add_files}',
      'REQUEST_CPUS = 1',
      'JOB_RELEASES = 1',
      'JOB_RELEASE_DELAY = 120',
      'LAUNCHER = "Rscript"',
      'SCRIPT = "get_biodiversity.R"',
      'ARGUMENTS = "%1"',
      'DATE_LABEL = "{DATE_LABEL}"',
      'RUN_AS_OWNER = {RUN_AS_OWNER}',
      'WAIT_FOR_RUN_COMPLETION = TRUE',
      'CLEAR_LINES = FALSE',
      'GET_OUTPUT = TRUE',
      'OUTPUT_DIR = "Output"',
      'OUTPUT_FILE = "biodiversity_{PROJECT}_{DATE_LABEL}.RData"',
      'CLUSTER_NUMBER_LOG = "{cluster_number_log}"'
    )

    config_path <- path(TEMP_DIR, "config_biodiv.R")

    # Write config file
    current_env <- environment()
    write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
    rm(config_template, current_env)

    prior_wd <- getwd()
    rc <- tryCatch ({
      setwd(path(CD,WD_BIODIVERSITY))
      system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
    },
    finally = {
      setwd(prior_wd)
    })

    if (rc != 0) stop("Condor run failed!")

    cluster_nr <- readr::parse_number(read_file(cluster_number_log))

    # Get GLOBIOM regions
    regions <- readRDS(path(CD,WD_BIODIVERSITY,"Input","globiom_regions.RData"))

    out_files <- dir_ls(path(CD,WD_BIODIVERSITY,"Output"),regexp = str_glue("biodiversity_{PROJECT}_{DATE_LABEL}_{cluster_nr}.*.RData"))
    
    # Get BII and cSAR aggregated outputs
    if (COMPUTE_BII) {
      cons_out_bii <- out_files %>%
        lapply(get_rds_out,2) %>% bind_rows() %>% left_join(scenario_mapping) %>%
        dplyr::select(-c(RegionName)) %>%
        rename(bii=score, bii_prod = score_prod)
    } else {
      cons_out_bii <- NULL
    }

    if (COMPUTE_cSAR){
      cons_out_csar <- out_files %>%
        lapply(get_rds_out,4) %>% bind_rows() %>% left_join(scenario_mapping) %>%
        dplyr::select(-c(RegionName)) %>%
        rename(csar=score)
    } else {
      cons_out_csar <- NULL
    }


    # Create output file
    if (COMPUTE_BII & COMPUTE_cSAR) {
      cons_out <- cons_out_bii %>% left_join(cons_out_csar) %>%
        rename(area=value, ScenYear=times,"BII" =  bii,"BII_PROD" =  bii_prod,"cSAR" =  csar) %>%
        dplyr::select(-c(ScenLoop,ScenNr,area)) %>%
        gather(Item,value,-c(REGION,SCEN1,SCEN2,SCEN3,ScenYear)) %>%
        relocate(Item,.after = REGION) %>% as_tibble() %>% spread(ScenYear,value)
    } else if (COMPUTE_BII & ! COMPUTE_cSAR) {
      cons_out <- cons_out_bii %>%
        rename(area=value, ScenYear=times,"BII" =  bii, "BII_PROD" =  bii_prod) %>%
        dplyr::select(-c(ScenLoop,ScenNr,area)) %>%
        gather(Item,value,-c(REGION,SCEN1,SCEN2,SCEN3,ScenYear)) %>%
        relocate(Item,.after = REGION) %>% as_tibble() %>% spread(ScenYear,value)
    } else if (! COMPUTE_BII & COMPUTE_cSAR) {
      cons_out <- cons_out_csar %>%
        rename(ScenYear=times,"cSAR" =  csar) %>%
        dplyr::select(-c(ScenLoop,ScenNr)) %>%
        gather(Item,value,-c(REGION,SCEN1,SCEN2,SCEN3,ScenYear)) %>%
        relocate(Item,.after = REGION) %>% as_tibble() %>% spread(ScenYear,value)
    }


    wgdx.reshape(cons_out, 6, symName="Biodiversity", setNames=c("REGION","Item","SCEN1","SCEN2","SCEN3"),
                 tName = "ScenYear", gdxName = path(CD,WD_BIODIVERSITY,"Output",str_glue("output_{out_id}.{i}.gdx")))

  }

  # Merge outputs into single gdx
  merge_gdx(PROJECT,path(CD,WD_BIODIVERSITY,"Output"),out_id,10^6)

  # Return the cluster number
  readr::parse_number(read_file(cluster_number_log))
}





#' Run output maps generation
#'
#' Produces netcdf files from downscaling outputs
#' @param cluster_nr_downscaling Cluster sequence number of the downscaling HTCondor submission
run_merge_and_transfer <- function(cluster_nr_downscaling) {

  # Get cluster number
  cluster_number_log <- path(TEMP_DIR, "cluster_number.log")

  config <- list()

  # Save config file
  config[[1]] <- cluster_nr_downscaling
  config[[2]] <- get_mapping()

  saveRDS(config,path(CD,WD_DOWNSCALING,"config.RData"))

  # Get downscaled files (!transfer is faster than using BUNDLE_ADDITIONAL_FILES)
  include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("downscaled_{cluster_nr_downscaling}.*.*"))

  exclude_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx")) %>%
    str_extract(str_glue("_[0-9]+.")) %>% unique() %>% na.omit()


  config_template <- c(
    'LABEL = "{PROJECT}"',
    'JOBS = c({str_c(SCENARIOS_FOR_DOWNSCALING, collapse=",")})',
    'REQUIREMENTS = c("R")',
    'REQUEST_MEMORY = 50000',
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
    'RUN_AS_OWNER = {RUN_AS_OWNER}',
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
    system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
  },
  finally = {
    setwd(prior_wd)
  })


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




#' Run merge and transfer for writing map - used when Pipeline was first developed; not used now and therefore commented out
#'
#' Sends data compilation from Downscaling to G4M to limpopo
#' @param cluster_nr_downscaling Cluster sequence number of the downscaling HTCondor submission
# run_merge_and_transfer <- function(cluster_nr_downscaling) {
#
#   # Get cluster number
#   cluster_number_log <- path(TEMP_DIR, "cluster_number.log")
#
#   config <- list()
#
#   # Define scenario count
#   scen_cnt <- tibble(scenid=1:length(SCENARIOS_FOR_DOWNSCALING),scencnt=as.integer(SCENARIOS_FOR_DOWNSCALING))
#
#   # Save config file
#   config[[1]] <- cluster_nr_downscaling
#   config[[2]] <- get_mapping()
#
#   saveRDS(config,path(CD,WD_DOWNSCALING,"config.RData"))
#
#   # Get downscaled files (!transfer is faster than using BUNDLE_ADDITIONAL_FILES)
#     include_files <- dir_ls(path(CD,WD_DOWNSCALING,"gdx"),regexp=str_glue("g4m_simu_out_{cluster_nr_downscaling}.*.*"))
#
#
#   config_template <- c(
#     'LABEL = "{PROJECT}"',
#     'JOBS = c({str_c(SCENARIOS_FOR_DOWNSCALING, collapse=",")})',
#     'REQUIREMENTS = c("R")',
#     'REQUEST_MEMORY = 50000',
#     'BUNDLE_EXCLUDE_DIRS = c("output", "prior_module", "source","t","postproc","renv","g4m_merge")',
#     'BUNDLE_EXCLUDE_FILES = c(".Rprofile","renv.lock","input/*.gdx","gdx/*.*")',
#     'BUNDLE_ADDITIONAL_FILES = ',
#     '{include_files}',
#     'REQUEST_CPUS = 1',
#     'JOB_RELEASES = 0',
#     'JOB_RELEASE_DELAY = 120',
#     'LAUNCHER = "Rscript"',
#     'SCRIPT = "write_maps.R"',
#     'ARGUMENTS = "%1"',
#     'DATE_LABEL = "{DATE_LABEL}"',
#     'RETAIN_BUNDLE = FALSE',
#     'RUN_AS_OWNER = {RUN_AS_OWNER}',
#     'WAIT_FOR_RUN_COMPLETION = TRUE',
#     'CLEAR_LINES = FALSE',
#     'GET_OUTPUT = TRUE',
#     'OUTPUT_DIR = "out"',
#     'OUTPUT_FILES = c("results_land_cover_CrpLnd.nc","results_land_cover_Grass.nc","results_land_cover_OthNatLnd.nc","results_land_cover_PltFor.nc","results_land_cover_forest_new_ha.nc","results_land_cover_forest_old_ha.nc","results_land_cover_arable.nc","results_land_cover_SS_area.nc","results_land_cover_urban.nc")'
#   )
#
#   config_path <- path(TEMP_DIR, "config_map.R")
#
#   # Write config file
#   current_env <- environment()
#   write_lines(lapply(config_template, .envir=current_env, str_glue), config_path)
#   rm(config_template, current_env)
#
#   prior_wd <- getwd()
#   rc <- tryCatch ({
#     setwd(WD_DOWNSCALING)
#     system(str_glue('Rscript --vanilla "{CD}/Condor_run_R/Condor_run_basic.R" "{config_path}"'))
#   },
#   finally = {
#     setwd(prior_wd)
#   })
#
#
# }


