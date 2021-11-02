

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
merge_gdx <- function(project, wd, c_nr, big_par) {

  merge_args <- c()
  merge_args <- c(merge_args, str_glue("output_",PROJECT,"_",c_nr,".*.gdx"))
  merge_args <- c(merge_args, str_glue("output=output_",PROJECT,"_",c_nr,"_merged.gdx"))
  merge_args <- c(merge_args, str_glue("big=",big_par))

  # Invoke GDXMERGE in the provided working directory
  prior_wd <- getwd()
  tryCatch({
    setwd(wd)
    rc <- system2("gdxmerge", args=merge_args)
    if (rc != 0) stop(str_glue("GDXMERGE failed with return code {rc}!"))
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

# Check for the occurrence of infeasibilities in GLOBIOM
check_sol <- function(cluster_nr_globiom){

  f <- path(CD,WD_GLOBIOM,"Model","gdx",str_glue("output_{PROJECT}_",cluster_nr_globiom,"_merged.gdx"))
  # Check for active ART_VARs
  artvar <- rgdx.param(f,"ARTVAR_ACTIVE")

  # Check for negative OF values
  obj <- rgdx.param(f,"Obj_Compare")

  # Retrieve scenarios with active ART_VAR
  artvar_values <- as.integer(str_sub(unique(artvar[,1]), start= -6))

  # Retrieve scenarios with negative OF
  negative_values <- as.integer(str_sub(unique(obj[which(obj$value < 0),1]), start= -6))

  if (length(artvar_values) > 0) {
    warning(str_glue("*************************************","\n","Scenario(s): ",
                     str_flatten(artvar_values,",")," has active ART_VAR(s), see log for details",
                     "\n","*************************************","\n"))

    # Get ART_VARs
    water <- rgdx.param(f,"ARTVAR_WATER_COMPARE")
    stover <- rgdx.param(f,"ARTVAR_STOVER_LU_COMPARE")
    biomass <- rgdx.param(f,"ARTVAR_MESSAGE_BIOMASS_COMPARE")
    process <- rgdx.param(f,"ARTVAR_COMPARE")
    affor <- rgdx.param(f,"ARTVAR_AFFOR_COMPARE")

    # Write to log file if active
    if (!dir_exists(path(CD,"log"))) dir_create(path(CD,"log"))
    log_f <- path(CD,"log",str_glue("{PROJECT}_{DATE_LABEL}_log.txt"))
    if (dim(water)[1] > 0)  write_lines(str_glue("ART_VAR WATER","\n"),log_f,append = T); write_delim(water,log_f,append = T)
    if (dim(stover)[1] > 0) write_lines(str_glue("ART_VAR STOVER","\n"),log_f,append = T); write_delim(stover,log_f,append = T)
    if (dim(biomass)[1] > 0) write_lines(str_glue("ART_VAR MESSAGE BIOMASS","\n"),log_f,append = T); write_delim(biomass,log_f,append = T)
    if (dim(process)[1] > 0) write_lines(str_glue("ART_VAR","\n"),log_f,append = T); write_delim(process,log_f,append = T)
    if (dim(affor)[1] > 0) write_lines(str_glue("ART_VAR AFFOR","\n"),log_f,append = T); write_delim(affor,log_f,append = T)
  }

  if (length(negative_values) > 0) {
    warning(str_glue("*************************************","\n","Scenario(s): ",
                     str_flatten(negative_values,","),"has negative objective function value",
                     "\n","*************************************","\n"))
  }
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


# Convert gdx globiom files to csv for G4M input - only for testing
gdx_to_csv_for_g4m <- function() {

  # Path to globiom outputs
  gdx_path <- path(CD,str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/output_globiom4g4mm_{PROJECT}_{DATE_LABEL}.gdx"))
  items <- c("G4Mm_SupplyResidues","G4Mm_SupplyWood","G4Mm_Wood_price","G4Mm_LandRent","G4Mm_CO2PRICE")

  # Read data tables and rearrange
  for(i in 1:length(items)){
    var <- as_tibble(rgdx.param(gdx_path,items[i]))
    if (i <= 3) names(var) <- c("G4MmItem","G4MmLogs","REGION","SCEN1","SCEN3","SCEN2","Year","value")
    if (i==4) names(var) <- c("LC_TYPE","REGION","SCEN1","SCEN3","SCEN2","Year","value")
    if (i==5) names(var) <- c("REGION","SCEN1","SCEN3","SCEN2","Year","value")

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

  save_environment("3c")

}

# Retrieve the mapping between globiom and downscaling scenarios
get_mapping <- function(){

  s_nr <-  sprintf("%06d", SCENARIOS[1])
  scen_map <- rgdx.set(path(CD,WD_GLOBIOM,"Model","gdx",str_glue("output_{PROJECT}_",cluster_nr_globiom,".",s_nr,".gdx")),"SCEN_MAP")

  scen_map <- scen_map  %>% na_if("") %>% na.omit
  scen_dims <- colnames(scen_map)

  loop_idx <- which(str_detect(scen_dims,regex("ScenLoop",ignore_case = T)))
  macro_idx <- which(str_detect(scen_dims,regex("SCEN1",ignore_case = T)))
  bioen_idx <- which(str_detect(scen_dims,regex("SCEN2",ignore_case = T)))
  iea_idx <- which(str_detect(scen_dims,regex("SCEN3",ignore_case = T)))

  colnames(scen_map)[c(loop_idx,macro_idx,bioen_idx,iea_idx)] <- c("ScenLoop","SCEN1","SCEN2","SCEN3")

  #Get solved scenarios
  scen_map_solved <- subset(scen_map,ScenLoop %in% SCENARIOS)
  scen_map_solved$SCEN2 <- str_replace_all(scen_map_solved$SCEN2,"\"","")

  # Define GLOBIOM - Downscaling map
  downs_input <- as_tibble(rgdx.param(path(str_glue(CD,"/",WD_DOWNSCALING,"/input/output_landcover_{PROJECT}_{DATE_LABEL}.gdx")),"LANDCOVER_COMPARE_SCEN"))
  names(downs_input) <- c("ScenNr","LC","SCEN1","SCEN2","SCEN3","Year","value")
  downs_input <- subset(downs_input,ScenNr=="World" & LC=="TotLnd" & Year==2000)
  downs_input <- downs_input %>% uncount(RESOLUTION_DOWNSCALING)
  downs_input$ScenNr <- 0:(length(downs_input$ScenNr)-1)
  downs_input <- downs_input[,c(1,3:5)]

  downscaling_scenarios <- merge(downs_input,scen_map_solved,by=c("SCEN1","SCEN2","SCEN3"))

  return(downscaling_scenarios)

}


# Generate G4M job string - new interface
get_g4m_jobs <- function(baseline = NULL){

  # Get downscaling mapping
  downs_map <-  unique(get_mapping()[-4])
  downs_map <- subset(downs_map, ScenLoop %in% SCENARIOS_FOR_G4M)

  # Save config files
  lab <- str_glue("{PROJECT}_{DATE_LABEL}")
  config <- list(lab,baseline,G4M_EXE)
  save(config, file=path(CD,WD_G4M,"Data","Default","config.RData"))
  save(downs_map, file=path(CD,WD_G4M,"Data","Default","scenario_map.RData"))

 # For now all scenarios are run in the baseline - needs to be adjusted with identifiers if same scenarios are run with different co2 price
  # Generate scenario string
  g4m_scenario_string <- ""
  if (baseline) {
    for (i in 1:dim(downs_map)[1]){
          s_str <- str_c(downs_map$SCEN1[i],"_",downs_map$SCEN3[i],"_",downs_map$SCEN2[i])
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",s_str," ",s_str," ",0,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  } else {
    for (i in 1:dim(downs_map)[1]){
          sb_str <- str_c(downs_map$SCEN1[i],"_",downs_map$SCEN3[i],"_",downs_map$SCEN2[i])
          s_str <- str_c(downs_map$SCEN1[i],"_",downs_map$SCEN3[i],"_",downs_map$SCEN2[i])
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",sb_str," ",s_str," ",-1,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  }

  return(g4m_scenario_string)
}

# Compile G4M
compile_g4m_model <- function(){

  # set path for exe file
  g4m_path <- path(CD,WD_G4M,G4M_EXE)

  # get source code path
  source_path <- path(CD,WD_G4M,"forest_gui_cell_glob_param_EPA_4_4_newInterfaceTest.cpp")

  if (!file_exists(g4m_path)){

    # get path to Rtools g++
    env_list <- Sys.getenv()
    rtools_path <- path(env_list[which(str_detect(env_list,"rtools") & !str_detect(env_list,";"))],"mingw32","bin","g++")

    # compile model
    compile_string <- str_glue(rtools_path," {source_path} -o {g4m_path}")
    system(compile_string)
  }

}

# Compile G4M output post-processing library
compile_table_merger <- function(){

  # Set wd
  prior_wd <- getwd()
  merger_path <- path(CD,WD_G4M,"tableMergeForLinker","merge_files.so")
  source_path <- path(CD,WD_G4M,"tableMergeForLinker","tableMerge_EPA_csv_linker.cpp")

  if (!file_exists(path(merger_path))) {
    # Compile C++ code
    compile_string <- str_glue("R CMD SHLIB -o {merger_path} ","{source_path} ")
    system(compile_string)
  }

}

# Compile outputs from g4m into csv file - temporary, will be changed to generate gdx?
generate_g4M_report <- function(file_path,file_suffix,scenarios,scenario_names,N,co2){

  # Call report generator
  .C('merge',path=as.character(file_path),
     suf=as.character(file_suffix),
     scen = as.character(scenarios),
     scen_name = as.character(scenario_names),
     Ns = as.integer(N),
     c_price = as.integer(co2))

}

# Save the global environment to continue execution in case of a session crash
save_environment <- function(step){

  # Create dir to store environment data
  if (!dir_exists(path(CD,"R","environment"))) dir_create(path(CD,"R","environment"))

  # Save global environment to file
  save.image(path(CD,"R","environment",str_glue("environment_{PROJECT}_{DATE_LABEL}_",step,".RData")))
}

# Remove global environment files
clear_environment <- function(){

  # Remove global environement files
  unlink(path(CD,"R","environment",str_glue("environment_{PROJECT}_{DATE_LABEL}_*.RData")))


}
