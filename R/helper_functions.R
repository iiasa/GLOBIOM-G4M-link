

# Extract a list of items from many GDX and return as tibble
batch_extract_tib <- function(items,files=NULL,gdxs=NULL){
  if(is.null(gdxs)){
    gdxs = lapply(files, gdx)
  }
  lall = list()
  for(item in items){
    tt = lapply(gdxs,gdxtools::extract,item,addgdx=F)
    tt = do.call("rbind",tt)
    tt = list(tt)
    names(tt) <- item
    tt <- as_tibble(tt)
    lall <- c(lall,tt)
  }
  return((lall))
}

# Merge gdx files if not set in the sample_config file
merge_gdx <- function(project, wd, c_nr) {

  merge_args <- c()
  merge_args <- c(merge_args, str_glue("output_",PROJECT,"_",c_nr,".*.gdx"))
  merge_args <- c(merge_args, str_glue("output=output_",PROJECT,"_",c_nr,"_merged.gdx"))

  # Invoke GDXMERGE in the provided working directory
  prior_wd <- getwd()
  tryCatch({
    setwd(wd)
    rc <- system2("gdxmerge", args=merge_args)
    stop(str_glue("GDXMERGE failed with return code {rc}!"))
  },
  finally = {
    setwd(prior_wd)
  })
}

# Merge gdx files from downscaling according to the scenario number
merge_gdx_down <- function(wd_out,s_list,s_cnt,c_nr,path_out){
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(wd_out)

    s_list <-  sprintf("%06d", s_list)
    merge_args <- c()
    merge_args <- c(merge_args, str_c(str_glue("downscaled_{PROJECT}_"),c_nr,".",s_list,".gdx"))
    merge_args <- c(merge_args, str_glue(str_glue("output=",path_out,"/output_landcover_{PROJECT}_",s_cnt,"_merged.gdx")))

    # Invoke GDX merge
    rc <- tryCatch(
      system2("gdxmerge", args=merge_args),
      error=function(e) e
    )
    if(rc != 0){
      stop(str_glue("GDXMERGE failed with return code {rc}!"))
    }

  },
  finally = {
    setwd(prior_wd)
  })
}

# Search and replace function for adapting GLOBIOM scripts
string_replace <- function(full_str,search_str,replace_str){
  # Search and replace using stringr
  if (any(str_detect(full_str,regex(search_str,ignore_case = T)))) {
    return(str_replace(full_str,regex(search_str,ignore_case = T),replace_str))
  } else {
    stop("Pattern not found, file configuration failed")
  }
}


# Function to convert gdx globiom files to csv for G4M input - only for testing
gdx_to_csv_for_G4M <- function() {

  # Read globiom outputs
  glob_files <- gdx(str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx"))
  items <- all_items(glob_files)$parameters

  # Read data tables and rearrange
  for(i in 1:length(items)){
    var <- as_tibble(glob_files[items[i]])
    if (i <= 3) names(var) <- c("G4MmItem","G4MmLogs","REGION","MacroScen","IEA_SCEN","BioenScen","Year","value")
    if (i==4) names(var) <- c("LC_TYPE","REGION","MacroScen","IEA_SCEN","BioenScen","Year","value")
    if (i==5) names(var) <- c("REGION","MacroScen","IEA_SCEN","BioenScen","Year","value")

    # Remap years to columns
    var2 <- var %>% spread(Year, value, fill = 0, convert = FALSE)

    if (i <= 3) var2 <- var2 %>% relocate(REGION, .before = G4MmItem)
    if (i==4) var2 <- var2 %>% relocate(REGION, .before = LC_TYPE)

    varname <- string_replace(items[i],"G4Mm_","")
    if (varname == "CO2PRICE") varname <- "CO2price"

    # Write csv files
    write.csv(var2, str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/GLOBIOM2G4M_output_{varname}_{PROJECT}_{DATE_LABEL}.csv"),
              row.names = F, quote = F)
  }

  # Read downscaling outputs and filter input data
  downs_files <- as_tibble(gdx(str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/downscaled_output_{PROJECT}_{DATE_LABEL}.gdx"))["LandCover_G4MID"])
  downs_files <- downs_files[,-1]
  downs_files <- subset(downs_files, V6 == "Reserved")
  downs_files <- downs_files[,-5]
  names(downs_files) <- c("g4m_05_id","MacroScen","IEA_SCEN","BioenScen","Year","value")

  # Remap years to columns
  downs2 <- downs_files %>% spread(Year, value, fill = 0, convert = FALSE)

  # Write csv file
  write.csv(downs2, str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/GLOBIOM2G4M_output_LC_abs_{PROJECT}_{DATE_LABEL}.csv"),
            row.names = F, quote = F)
}

# Function to retrieve the mapping between globiom and downscaling scenarios
get_mapping <- function(){


  # Read scenario mapping from globiom and create data frame
  globiom_scenario_map <- read_lines(path(str_glue(CD,"/",WD_GLOBIOM,"/Model/{GLOBIOM_SCEN_FILE}")))

  # Find start and end of scenario definition
  idx_start <- which(str_detect(globiom_scenario_map,regex("SCEN_MAP[:print:]+AllScenLOOP[:print:]+",ignore_case = T)))
  idx_end <- which(str_detect(globiom_scenario_map[(idx_start+2):length(globiom_scenario_map)],"/[:print:]+"))[1]+idx_start

  # Get number of dimensions
  scen_dims <- globiom_scenario_map[idx_start]
  scen_dims <- unlist(str_split(scen_dims,"[(),]"))
  scen_dims <- scen_dims[which(!scen_dims %in% c(regex("SCEN_MAP", ignore_case = T),""))]

  # Get mapping
  N <- length(scen_dims)
  scen_map <- globiom_scenario_map[(idx_start+1):(idx_end)]
  scen_map <- str_replace_all(scen_map, "[ \t\n\r\v\f]+", "")
  scen_map <- as_tibble(str_split_fixed(scen_map, "\\.",n=N), .name_repair="minimal")
  names(scen_map) <- scen_dims
  scen_map <- scen_map  %>% na_if("") %>% na.omit

  loop_idx <- which(str_detect(scen_dims,regex("ScenLoop",ignore_case = T)))
  macro_idx <- which(str_detect(scen_dims,regex("MacroScen",ignore_case = T)))
  bioen_idx <- which(str_detect(scen_dims,regex("BioenScen",ignore_case = T)))
  iea_idx <- which(str_detect(scen_dims,regex("IEA[:print:]+Scen",ignore_case = T)))

  colnames(scen_map)[c(loop_idx,macro_idx,bioen_idx,iea_idx)] <- c("ScenLoop","MacroScen","BioenScen","IEAScen")

  #Get solved scenarios
  scen_map_solved <- subset(scen_map,ScenLoop %in% SCENARIOS)
  scen_map_solved$BioenScen <- str_replace_all(scen_map_solved$BioenScen,"\"","")

  # Define GLOBIOM - Downscaling map
  downs_input <- as_tibble(gdx(path(str_glue(CD,"/",WD_DOWNSCALING,"/input/output_landcover_{PROJECT}_{DATE_LABEL}.gdx")))["LANDCOVER_COMPARE_SCEN"])
  names(downs_input) <- c("ScenNr","LC","MacroScen","BioenScen","IEAScen","Year","value")
  downs_input <- subset(downs_input,ScenNr=="World" & LC=="TotLnd" & Year==2000)
  downs_input <- downs_input %>% uncount(RESOLUTION_DOWNSCALING)
  downs_input$ScenNr <- 0:(length(downs_input$ScenNr)-1)
  downs_input <- downs_input[,c(1,3:5)]

  downscaling_scenarios <- merge(downs_input,scen_map_solved,by=c("MacroScen","BioenScen","IEAScen"))

  return(downscaling_scenarios)

}


# Function to generate G4M job string
get_g4m_jobs <- function(baseline = NULL) {

  # Get downscaling mapping
  downs_map <-  unique(get_mapping()[-4])

  # Read in scenario data and sort the baseline scenarios
  f <- path(str_glue(CD,"/",PATH_FOR_G4M,"/output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx"))
  downs_results <- as_tibble(gdx(f)["G4Mm_CO2PRICE"])
  names(downs_results) <- c("Region","MacroScen","IEAScen","BioenScen","Year","value")
  final_year <- max(downs_results$Year)
  downs_results_base <- subset(downs_results, Year==final_year)

  # Define IEA baselines and build scenario string - current same for baseline and final scenarios, will
  # be modified according to standardized baseline/scenario runs in the future
  macro <- unique(downs_results$MacroScen)
  iea <- unique(downs_results$IEAScen)
  base_scenarios <- unique(downs_results$BioenScen) #unique(downs_results_base$BioenScen[which(downs_results_base$value==0.01)])
  g4M_scenarios <- unique(downs_results$BioenScen)

  # Generate scenario string
  g4m_scenario_string <- ""
  if (baseline) {
    for (i in 1:length(macro)){
      for (j in 1:length(iea)){
        for (k in 1:length(base_scenarios)){

            l <- str_c(macro[i]," ",iea[j]," ",base_scenarios[k]," ",0,",")
            g4m_scenario_string <- c(g4m_scenario_string,l)
          }
        }
      }
  } else {
    for (i in 1:length(macro)){
      for (j in 1:length(iea)){
        for (k in 1:length(g4M_scenarios)){

          l <- str_c(macro[i]," ",iea[j]," ",g4M_scenarios[k]," ",-1,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
        }
      }
    }
  }

  return(g4m_scenario_string)
}


# Function to generate G4M job string - new interface
get_g4m_jobs_new <- function(baseline = NULL){

  # Get downscaling mapping
  downs_map <-  unique(get_mapping()[-4])
  downs_map <- subset(downs_map, ScenLoop %in% SCENARIOS_FOR_G4M)

  g4m_scen_table <- downs_map %>% relocate(BioenScen, .after = IEAScen)

 # For now all scenarios are run in the baseline - needs to be adjusted with identifiers if same scenarios are run with different co2 price
  # Generate scenario string
  g4m_scenario_string <- ""
  if (baseline) {
    for (i in 1:dim(downs_map)[1]){
          s_str <- str_c(downs_map$MacroScen[i],"_",downs_map$IEAScen[i],"_",downs_map$BioenScen[i])
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",s_str," ",s_str," ",0,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  } else {
    for (i in 1:dim(downs_map)[1]){
          sb_str <- str_c(downs_map$MacroScen[i],"_",downs_map$IEAScen[i],"_",downs_map$BioenScen[i])
          s_str <- str_c(downs_map$MacroScen[i],"_",downs_map$IEAScen[i],"_",downs_map$BioenScen[i])
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",sb_str," ",s_str," ",-1,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  }

  # # Read in scenario data and sort the baseline scenarios
  # f <- path(str_glue(CD,"/",PATH_FOR_G4M,"/output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx"))
  # downs_results <- as_tibble(gdx(f)["G4Mm_CO2PRICE"])
  # names(downs_results) <- c("Region","MacroScen","IEAScen","BioenScen","Year","value")
  # final_year <- max(downs_results$Year)
  # downs_results_base <- subset(downs_results, Year==final_year)
  #
  # # Define IEA baselines and build scenario string - current same for baseline and final scenarios, will
  # # be modified according to standardized baseline/scenario runs in the future
  # macro <- unique(downs_results$MacroScen)
  # iea <- unique(downs_results$IEAScen)
  # base_scenarios <- unique(downs_results$BioenScen) #unique(downs_results_base$BioenScen[which(downs_results_base$value==0.01)])
  # g4M_scenarios <- unique(downs_results$BioenScen)
  #
  # # Generate scenario string
  # g4m_scenario_string <- ""
  # if (baseline) {
  #   for (i in 1:length(macro)){
  #     for (j in 1:length(iea)){
  #       for (k in 1:length(base_scenarios)){
  #         s_str <- str_c(macro[i],"_",iea[j],"_",base_scenarios[k])
  #         l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",s_str," ",s_str," ",0,",")
  #         g4m_scenario_string <- c(g4m_scenario_string,l)
  #       }
  #     }
  #   }
  # } else {
  #   for (i in 1:length(macro)){
  #     for (j in 1:length(iea)){
  #       for (k in 1:length(g4M_scenarios)){
  #         sb_str <- str_c(macro[i],"_",iea[j],"_",base_scenarios[k])
  #         s_str <- str_c(macro[i],"_",iea[j],"_",g4M_scenarios[k])
  #         l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",sb_str," ",s_str," ",-1,",")
  #         g4m_scenario_string <- c(g4m_scenario_string,l)
  #       }
  #     }
  #   }
  # }

  return(g4m_scenario_string)
}

# Function to compile outputs from g4m into csv file - temporary, will be changed to generate gdx?
generate_g4M_report <- function(file_path,file_suffix,scenarios,scenario_names,N,co2){

  # Call report generator
  .C('merge',path=as.character(file_path),
     suf=as.character(file_suffix),
     scen = as.character(scenarios),
     scen_name = as.character(scenario_names),
     Ns = as.integer(N),
     c_price = as.integer(co2))

}

