# Functions for transferring data between models

#' Function to transfer the downscaling output to G4M folder. Merges the downscaled
#' regions if required
#'
#' @param cluster_nr_downscaling Cluster sequence number of prior downscaling HTCondor submission
merge_and_transfer <- function(cluster_nr_downscaling) {
  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (MERGE_GDX_DOWNSCALING){

    # Save merged output to G4M folder
    f <- str_glue(WD_DOWNSCALING,"/gdx/downscaled_{PROJECT}_{cluster_nr_downscaling}_merged.gdx")
    file_copy(f, path(CD, PATH_FOR_G4M, str_glue("downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")), overwrite = TRUE)
  } else {

    # Get scenario mapping and indices
    scenario_mapping <- get_mapping()
    scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING)]

    # Extract land cover table for G4M
    for (i in 1:length(scenarios_idx)){

      # Get scenario number
      s_list <-  sprintf("%06d", scenarios_idx[i])

      # Read data for G4M
      if (i==1) {
        out <- gdx(path(CD,WD_DOWNSCALING,"gdx", str_glue("{GDX_OUTPUT_NAME}_{PROJECT}_{cluster_nr}.",
                                                          s_list,".gdx")))["LandCover_G4MID"]
      } else {

        out_aux <-  gdx(path(CD,WD_DOWNSCALING,"gdx", str_glue("{GDX_OUTPUT_NAME}_{PROJECT}_{cluster_nr}.",
                                                               s_list,".gdx")))["LandCover_G4MID"]
        out <- rbind(out,out_aux)

      }

    }

    # Write merged gdx file to G4M data folder
    write.gdx(path(CD, PATH_FOR_G4M, str_glue("downscaled_output_{PROJECT}_{DATE_LABEL}.gdx")),
              list(LandCover_G4MID=out))

  }
}

#' Function to collect G4M results from binary files and write a csv file for GLOBIOM
#;
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
compile_g4m_data <- function(baseline = NULL) {
  if (!is.logical(baseline))
    stop("Set baseline parameter to TRUE or FALSE!")

  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(WD_G4M)

    # Path of output folder
    file_path <- path_wd("out", str_glue("{PROJECT}_{DATE_LABEL}"))

    # Suffix of scenario runs
    file_suffix <- str_glue("_{PROJECT}_{DATE_LABEL}_") # for now in future should be indexed by project and date label

    # G4M scenario ID
    g4m_jobs <- get_g4m_jobs_new(baseline = baseline)[-1]

    # Split dimensions and extract scenario name
    scenarios_split <- str_split_fixed(g4m_jobs," ",n=4)
    scenarios <- scenario_names <- scenarios_split[,3]

    # Number of scenarios
    N <- length(scenarios)

    # Extract CO2 price
    co2 <- abs(as.integer(str_replace(scenarios_split[,4],",","")))

    # Compile results
    generate_g4M_report(file_path, file_suffix, scenarios, scenario_names, N, co2)

    # Edit and save csv file for GAMS
    g4m_out <- read.csv(path(file_path, G4M_FEEDBACK_FILE))
    colnames(g4m_out)[1:3] <- ""
    colnames(g4m_out) <- str_replace_all(colnames(g4m_out),"X","")
    write.csv(g4m_out, path(file_path, G4M_FEEDBACK_FILE), row.names = F, quote = T)
  },
  finally = {
    setwd(prior_wd)
  })
}
