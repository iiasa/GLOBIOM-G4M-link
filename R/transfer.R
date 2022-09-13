# Functions for transferring data between models

#' Transfer the downscaling output to G4M folder.
#'
#' @param cluster_nr_downscaling Cluster sequence number of prior downscaling HTCondor submission
merge_and_transfer <- function(cluster_nr_downscaling) {

  save_environment("3a")

  # Define parameters
  MERGE_GDX_DOWNSCALING = FALSE

  # Transfer gdx to G4M folder - in case files were merged on limpopo
  if (MERGE_GDX_DOWNSCALING){

    # Get downscaling output
    downs_files <- rgdx.param(path(CD,WD_DOWNSCALING,"gdx",
                                   str_glue("downscaled_{PROJECT}_{cluster_nr_downscaling}_merged.gdx")),"LandCover_G4MID")

    # Select data for G4M
    if (dim(downs_files)[2] == 8) downs_files <- downs_files[,-1]

    names(downs_files) <- c("g4m_05_id","SCEN1","SCEN3","SCEN2","LC","Year","value")

    downs_files <- downs_files %>% filter(LC == "Reserved") %>% dplyr::select(-LC)

    # Remap years to columns
    downs2 <- downs_files %>% spread(Year, value, fill = 0, convert = FALSE) %>%
      mutate(SCEN1=toupper(SCEN1),SCEN2=toupper(SCEN2),SCEN3=toupper(SCEN3))

    # Save to G4M folder
    write_csv(downs2, path(CD,str_glue("{WD_G4M}"),"Data","GLOBIOM",str_glue("{PROJECT}_{DATE_LABEL}"),
                           str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}.csv")),
              col_names = T)

  } else {

    # Get scenario mapping and indices
    scenario_mapping <- get_mapping()

    # scenario counter
    scen_cnt <- 1

    for (k in 1: length(SCENARIOS_FOR_DOWNSCALING)){

      scenarios_idx <- scenario_mapping$ScenNr[which(scenario_mapping$ScenLoop %in% SCENARIOS_FOR_DOWNSCALING[k])] %>% sort()

      # Extract land cover table for G4M
      for (i in 1:length(scenarios_idx)){

        # Get scenario number
        s_list <-  sprintf("%06d", scenarios_idx[i])

        # Read data for G4M
        if (!DOWNSCALING_TYPE=="downscalr"){
          downs_files <- rgdx.param(path(CD,WD_DOWNSCALING,"gdx", str_glue("downscaled_{PROJECT}_{cluster_nr_downscaling}.",
                                                                           s_list,".gdx")),"LandCover_G4MID")
          # Select data for G4M
          if (dim(downs_files)[2] == 8) downs_files <- downs_files[,-1]
          names(downs_files) <- c("g4m_05_id","SCEN1","SCEN3","SCEN2","LC","Year","value")

          downs_files <- downs_files %>% filter(LC == "Reserved") %>% dplyr::select(-LC)

          names(downs_files) <- c("g4m_05_id","SCEN1","SCEN3","SCEN2","Year","value")

          # Remap years to columns
          downs2 <- downs_files %>% spread(Year, value, fill = 0, convert = FALSE) %>%
            mutate(SCEN1=toupper(SCEN1),SCEN2=toupper(SCEN2),SCEN3=toupper(SCEN3))

          # Construct file path
          f <- path(CD,str_glue("{WD_G4M}"),"Data","GLOBIOM",str_glue("{PROJECT}_{DATE_LABEL}"),
                    str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}_{scen_cnt}.csv"))

          if (i==1) {
            # Write csv file
            write_csv(downs2, f, col_names = T)
          } else {
            # Append to csv file
            write_csv(downs2, f, append = T)
          }

        } else {

          downs_files <- readRDS(path(CD,WD_DOWNSCALING,"gdx", str_glue("output_{PROJECT}_{cluster_nr_downscaling}.",
                                                                        s_list,".RData")))[[2]] %>% select(-LC_TYPES_EPIC)

          f <- path(CD,str_glue("{WD_G4M}"),"Data","GLOBIOM",str_glue("{PROJECT}_{DATE_LABEL}"),
                    str_glue("GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}_{scen_cnt}.csv"))

          if (i==1) {
            # Write csv file
            write_csv(downs_files, f, col_names = T)
          } else {
            # Append to csv file
            write_csv(downs_files, f, append = T)
          }

        }


      }
      scen_cnt <- scen_cnt + 1
    }
  }


  # Save global environment
  save_environment("3b")

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
    file_inpath <- path_wd("out", str_glue("{PROJECT}_{DATE_LABEL}"))
    file_outpath <- path(CD,WD_GLOBIOM,"Model","output","g4m",str_glue("{PROJECT}_{DATE_LABEL}"))
    if (!dir_exists(file_outpath))  dir_create(file_outpath)
    if(file_exists(path(file_outpath,G4M_FEEDBACK_FILE))) file_delete(path(file_outpath,G4M_FEEDBACK_FILE))

    # Suffix of scenario runs
    file_suffix <- str_glue("_{PROJECT}_{DATE_LABEL}_") # for now in future should be indexed by project and date label

    # G4M scenario ID
    g4m_jobs <- get_g4m_jobs(baseline = baseline)[-1]

    # Split dimensions and extract scenario name
    scenarios_split <- str_split_fixed(g4m_jobs," ",n=4)
    scenarios <- scenario_names <- scenarios_split[,3]

    # Number of scenarios
    N <- length(scenarios)

    # Extract CO2 price
    co2 <- abs(as.integer(str_replace(scenarios_split[,4],",","")))

    # Compile results
    generate_g4M_report(file_inpath, file_outpath, file_suffix, scenarios, scenario_names, N, co2)

  },
  finally = {
    setwd(prior_wd)
    save_environment("5b")
  })
}
