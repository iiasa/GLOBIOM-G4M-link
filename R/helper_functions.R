
#' Merge gdx files if not set in the sample_config file
#'
#' @param project project label
#' @param wd directory of gdx files to be merged
#' @param c_nr limpopo cluster number
#' @param big_par BIG parameter (integer) to merge large gdx files
merge_gdx <- function(project, wd, c_nr, big_par) {

  # Define arguments
  merge_args <- c()
  merge_args <- c(merge_args, str_glue("output_", c_nr, ".*.gdx"))
  merge_args <- c(merge_args, str_glue("output=output_", project, "_", c_nr, "_merged.gdx"))
  merge_args <- c(merge_args, str_glue("big=", big_par))

  # Invoke GDXMERGE in the provided working directory
  prior_wd <- getwd()
  tryCatch({
    setwd(wd)
    rc <- system2("gdxmerge", args = merge_args)
    if (rc != 0)
      stop(str_glue("GDXMERGE failed with return code {rc}!"))
  },
  finally = {
    setwd(prior_wd)
  })
}

#' Merge gdx files from downscaling according to the scenario number
#'
#' @param wd_out directory of gdx files
#' @param s_list list of scenarios to be merged
#' @param s_cnt identifier for the merged output
#' @param c_nr limpopo cluster number
#' @param path_out output directory for the merged gdx
merge_gdx_down <- function(wd_out, s_list, s_cnt, c_nr, path_out){
  prior_wd <- getwd()
  rc <- tryCatch ({
    setwd(wd_out)

    s_list <-  sprintf("%06d", s_list)
    merge_args <- c()
    merge_args <- c(merge_args, str_c(str_glue("downscaled_"), c_nr, ".", s_list, ".gdx"))
    merge_args <- c(merge_args, str_glue(str_glue("output=", path_out, "/output_landcover_{PROJECT}_", s_cnt, "_merged.gdx")))

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

#' Check for the occurrence of infeasibilities in GLOBIOM solution
#'
#' @param cluster_nr_globiom Cluster sequence number of prior GLOBIOM HTCondor submission
check_sol <- function(cluster_nr_globiom){

  f <- path(CD,WD_GLOBIOM,"Model", "gdx", str_glue("output_{PROJECT}_", cluster_nr_globiom, "_merged.gdx"))
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

#' Search and replace function for adapting GLOBIOM scripts
#'
#' @param full_str string input
#' @param search_str search pattern in input
#' @param replace_str replace pattern in input
string_replace <- function(full_str,search_str,replace_str){
  # Search and replace using stringr
  if (any(str_detect(full_str,regex(search_str,ignore_case = T)))) {
    return(str_replace(full_str,regex(search_str,ignore_case = T),replace_str))
  } else {
    stop("Pattern not found, file configuration failed")
  }
}


#' Search and replace function for adapting GLOBIOM scripts
#'
#' @param full_str string input
#' @param search_str search pattern in input
#' @param replace_str replace pattern in input
string_replace_all <- function(full_str,search_str,replace_str){
  # Search and replace using stringr
  if (any(str_detect(full_str,regex(search_str,ignore_case = T)))) {
    return(str_replace_all(full_str,regex(search_str,ignore_case = T),replace_str))
  } else {
    stop("Pattern not found, file configuration failed")
  }
}

#' Convert gdx globiom files to csv for G4M input
#'
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

    # Harmonize scenario names
    var2 <- var2 %>% mutate(SCEN1=toupper(SCEN1),SCEN2=toupper(SCEN2),SCEN3=toupper(SCEN3))

    # Write csv files
    write.csv(var2, str_glue("{WD_G4M}/Data/GLOBIOM/{PROJECT}_{DATE_LABEL}/GLOBIOM2G4M_output_{varname}_{PROJECT}_{DATE_LABEL}.csv"),
              row.names = F, quote = F)
  }

  save_environment("3c")

}

#' Retrieve the mapping between globiom and downscaling scenarios
#'
get_mapping <- function(){

  s_nr <-  sprintf("%06d", SCENARIOS[1])
  scen_map <- rgdx.set(path(CD,WD_GLOBIOM,"Model","gdx",str_glue("output_",cluster_nr_globiom,".",s_nr,".gdx")),"SCEN_MAP")

  scen_map <- scen_map  %>% na_if("") %>% na.omit
  scen_dims <- colnames(scen_map)

  loop_idx <- which(str_detect(scen_dims,regex("ScenLoop",ignore_case = T)))
  macro_idx <- which(str_detect(scen_dims,regex("SCEN1",ignore_case = T)))
  bioen_idx <- which(str_detect(scen_dims,regex("SCEN2",ignore_case = T)))
  iea_idx <- which(str_detect(scen_dims,regex("SCEN3",ignore_case = T)))

  colnames(scen_map)[c(loop_idx,macro_idx,bioen_idx,iea_idx)] <- c("ScenLoop","SCEN1","SCEN2","SCEN3")

  #Get solved scenarios
  scen_map_solved <- scen_map %>% filter(ScenLoop %in% SCENARIOS) %>%
    mutate(SCEN1=toupper(SCEN1),SCEN2=toupper(SCEN2),SCEN3=toupper(SCEN3))

  scen_map_solved$SCEN2 <- str_replace_all(scen_map_solved$SCEN2,"\"","")

  # Define GLOBIOM - Downscaling map
  downs_input <- as_tibble(rgdx.param(path(str_glue(CD,"/",WD_DOWNSCALING,"/input/output_landcover_{PROJECT}_{DATE_LABEL}.gdx")),"LANDCOVER_COMPARE_SCEN"))
  names(downs_input) <- c("ScenNr","LC","SCEN1","SCEN2","SCEN3","Year","value")
  downs_input <- downs_input %>% filter(ScenNr=="World" & LC=="TotLnd" & Year==2000) %>% uncount(RESOLUTION_DOWNSCALING)
  downs_input$ScenNr <- 0:(length(downs_input$ScenNr)-1)
  downs_input <- downs_input %>% dplyr::select(c(ScenNr,SCEN1,SCEN2,SCEN3)) %>%
    mutate(SCEN1=toupper(SCEN1),SCEN2=toupper(SCEN2),SCEN3=toupper(SCEN3))

  scen_map_solved$SCEN3 <- scen_map_solved$SCEN3 %>% toupper()
  downs_input$SCEN3 <- downs_input$SCEN3 %>% toupper()

  downscaling_scenarios <- merge(downs_input,scen_map_solved,by=c("SCEN1","SCEN2","SCEN3"))

  return(downscaling_scenarios)

}


#' Prepare data fro G4M when using downscalr for the downscaling step (Authored by Michael Wögerer)
#'
#' @param downscalr_res data table returned by downscalr
#' @param curr.SCEN1 scenario dimension 1
#' @param curr.SCEN2 scenario dimension 2
#' @param curr.SCEN3 scenario dimension 3
#' @param out.path output path

G4M_link <- function(downscalr_res, curr.SCEN1, curr.SCEN2, curr.SCEN3){

  mapping <- readRDS(file='source/g4m_mapping.RData')[[1]]
  mapping <- data.frame(apply(mapping, 2, as.numeric))

  dat <- downscalr_res$out.res
  dat$ns <- as.numeric(dat$ns)
  dat <- dat %>% left_join(mapping, by=c("ns"="SimUID")) %>% group_by(g4m_05_id, lu.from, times) %>%
    summarise(value=sum(value)) %>% rename("ScenYear"="times", "LC_TYPES_EPIC"= "lu.from") %>%
    mutate(LC_TYPES_EPIC=recode(LC_TYPES_EPIC, "Forest"="unreserved",
                                "OthNatLnd"="unreserved",
                                "PriFor"="unreserved",
                                "MngFor"="unreserved",
                                "protected_priforest"="unreserved",
                                "protected_other"="unreserved",)) %>%
    subset(LC_TYPES_EPIC!="unreserved") %>% ungroup() %>% group_by(g4m_05_id, ScenYear) %>%
    summarise(value=sum(value)) %>% na.omit()

  dat <- bind_cols(g4m_05_id=dat$g4m_05_id, SCEN1=curr.SCEN1, SCEN3=curr.SCEN3,SCEN2=curr.SCEN2,
                   LC_TYPES_EPIC="Reserved", ScenYear=dat$ScenYear, value=dat$value) %>%
                   pivot_wider(id_cols=c(g4m_05_id, SCEN1, SCEN3, SCEN2, LC_TYPES_EPIC),
                               names_from = "ScenYear", values_from = "value")


  return(dat)

}


#' Write gdx files for G4M from downsalr results
#'
#'
export_gdx_for_g4m <- function(data,data_for_g4m){

  attr(data, "symName") <- "Land_Cover_SU_Region_SCEN"
  symDim <- 7
  LC <- wgdx.reshape(data, symDim, symName = "Land_Cover_SU_Region_SCEN", tName = "ScenYear")

  attr(data_for_g4m, "symName") <- "LandCover_G4MID"
  symDim <- 6
  G4M <- wgdx.reshape(data_for_g4m, symDim, symName = "LandCover_G4MID", tName = "ScenYears")

  ## Not run:
  # send the data to GDX
  wgdx.lst(path(CD,WD_DOWNSCALING,"gdx"), LC,G4M[[7]])
}


#' Generate G4M job string for the new G4M interface
#'
#' @param baseline = TRUE|FALSE: set to TRUE to select baseline scenarios.
get_g4m_jobs <- function(baseline = NULL){

  # Get downscaling mapping
  downs_map <-  get_mapping() %>% dplyr::select(-ScenNr) %>%
                filter(ScenLoop %in% SCENARIOS_FOR_G4M) %>% unique()

  # Save config files
  lab <- str_glue("{PROJECT}_{DATE_LABEL}")
  config <- list(lab,baseline,G4M_EXE,BASE_SCEN1,BASE_SCEN2,BASE_SCEN3)
  save(config, file=path(CD,WD_G4M,"Data","Default","config.RData"))
  save(downs_map, file=path(CD,WD_G4M,"Data","Default","scenario_map.RData"))

 # For now all scenarios are run in the baseline - needs to be adjusted with identifiers if same scenarios are run with different co2 price
  # Generate scenario string
  g4m_scenario_string <- ""
  if (baseline) {
    for (i in 1:dim(downs_map)[1]){
          s_str <- str_glue("{BASE_SCEN1}_{BASE_SCEN3}_{BASE_SCEN2}")
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",s_str," ",s_str," ",0,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  } else {
    for (i in 1:dim(downs_map)[1]){
          sb_str <- str_glue("{BASE_SCEN1}_{BASE_SCEN3}_{BASE_SCEN2}")
          s_str <- str_c(downs_map$SCEN1[i],"_",downs_map$SCEN3[i],"_",downs_map$SCEN2[i])
          l <- str_c(str_glue("{PROJECT}_{DATE_LABEL}")," ",sb_str," ",s_str," ",-1,",")
          g4m_scenario_string <- c(g4m_scenario_string,l)
    }
  }

  return(g4m_scenario_string)
}

#' Compile G4M model from source code
#'
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

#' Compile G4M output post-processing library from source code
#'
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
generate_g4M_report <- function(file_inpath,file_outpath,file_suffix,scenarios,scenario_names,N,co2){

  # Call report generator
  .C('merge',inpath=as.character(file_inpath),
     outpath=as.character(file_outpath),
     suf=as.character(file_suffix),
     scen = as.character(scenarios),
     scen_name = as.character(scenario_names),
     Ns = as.integer(N),
     c_price = as.integer(co2))

}

#' Save the global environment to continue execution in case of a session crash
#'
#' @param step identifier for the global environment at the focal chunk
save_environment <- function(step){

  # Create dir to store environment data
  if (!dir_exists(path(CD,"R","environment"))) dir_create(path(CD,"R","environment"))

  # Save global environment to file
  save.image(path(CD,"R","environment",str_glue("environment_{PROJECT}_{DATE_LABEL}_",step,".RData")))
}

#' Remove G4M outputs not used in the link
#'
clear_glob_files <- function(){

  # Remove GLOBIOM files
  unlink(path(CD,WD_GLOBIOM,"gdx","*.*"),recursive = TRUE)

}

#' Remove Downscaling outputs not used in the link
#'
clear_downs_files <- function(){

  # Remove downscaling files
  unlink(path(CD,WD_DOWNSCALING,"gdx","*.*"),recursive = TRUE)

}

#' Remove G4M outputs not used in the link
#'
clear_g4m_files <- function(){

  # Remove unused G4M files
  unlink(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}"),"*.*"),recursive = TRUE)
  unlink(path(CD,WD_G4M,"out",str_glue("{PROJECT}_{DATE_LABEL}"),"baseline","*.*"),recursive = TRUE)

}

#' Remove global environment files
#'
clear_environment <- function(){

    # Remove global environment files
  unlink(path(CD,"R","environment",str_glue("environment_{PROJECT}_{DATE_LABEL}_*.RData")))

}

#' Transfer final gdx to output folder
#'
transfer_outputs <- function(){

  out_file <- dir_ls(path(CD,WD_GLOBIOM,"Model","output","iamc"),regexp=str_glue("Output4_IAMC_template_{PROJECT}_{DATE_LABEL}*.*gdx"))
  new_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"),str_glue("Output4_IAMC_template_{PROJECT}_{DATE_LABEL}.gdx"))

  # Transfer gdx to output folder
  file_move(out_file,new_dir)

}


#' Compute G4M results at the SimU level
#'
#'
g4m_to_simu <- function(){

  # Load G4M output conversion function
  source(path(WD_DOWNSCALING,"G4M_DS_to_simu_link_final_v2.R"))

  # Get downscaling mapping
  downs_map <-  get_mapping() %>% dplyr::select(-REGION) %>%
    filter(ScenLoop %in% SCENARIOS_FOR_G4M)

  for (i in 1:dim(downs_map)[1]){

    # Define loading function
    rfunc <- function(x) readRDS(x)[[3]]$out.res

    # Define scenarios
    dscens <- downs_map %>% slice(i)

    # Load in downscalr output
    downscalr_out <- rfunc(path(WD_DOWNSCALING,"gdx",
                            str_glue("output_",cluster_nr_downscaling,".",
                                              sprintf("%06d",dscens$ScenNr),".RData")))

    # Get output
    g4m_simu_out <- g4mid_to_simuid(downscalr_out,DATE_LABEL,PROJECT,dscens$SCEN1,
                                    dscens$SCEN2,dscens$SCEN3)

    # Write output
    outpath <- path(WD_G4M,"out",str_glue(PROJECT,"_",DATE_LABEL),"simu_id")

    if (!dir_exists(outpath)) dir_create(outpath)

    saveRDS(g4m_simu_out,path(outpath,str_glue("g4m_simu_out","_",PROJECT,"_",cluster_nr_downscaling,".",
                                                 sprintf("%06d",dscens$ScenNr),".RData")))

  }

}

#' Remove downscaling temporary post-processing outputs
#'
clear_g4mtosimu_files <- function(){

  # Remove unused G4M files
  unlink(path(CD,WD_DOWNSCALING,"postproc"),recursive = TRUE)

}


#' Plot GLOBIOM and lookup table results
#'
#' @param scenarios_for_lookup scenario selection for lookup table plots
#' @param scenarios_for_globiom scenario selection for GLOBIOM plots
plot_results <- function(scenarios_for_lookup,scenarios_for_globiom){

  # Create plot directory
  plot_dir <- path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"),"plots")
  if(!dir_exists(plot_dir)) dir_create(plot_dir)

  # Read lookup table results
  out_lookup <- rgdx.param(path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"),
                                str_glue("Output4_IAMC_template_{PROJECT}_{DATE_LABEL}.gdx")),"OUTPUT4_SSP")

  # Read global lookup table results
  out_lookup_world <- rgdx.param(path(CD,"output",str_glue("{PROJECT}_{DATE_LABEL}"),
                                str_glue("Output4_IAMC_template_{PROJECT}_{DATE_LABEL}.gdx")),"OUTPUT4_SSP_AG")

  # Read GLOBIOM output table
  out_globiom <- as_tibble(rgdx.param(path(CD,WD_GLOBIOM,"Model","gdx",
                                           str_glue("output_{PROJECT}_{cluster_nr_globiom}_merged.gdx")),"OUTPUT")[,-1])

  # Read FAO data
  out_fao <- read_csv(path(CD,WD_GLOBIOM,"Model","finaldata","OUTPUT_FAO_REGION_since1961.csv"),
                      col_types =list("c","c","c","c","c","c","c","i","d"),col_names = F,quote = "'")

  # Edit table names
  colnames(out_fao) <- colnames(out_globiom) <- c("VAR_ID","UNIT","REGION","ITEM","SCEN1","SCEN2","SCEN3","YEAR","value")
  colnames(out_lookup) <- colnames(out_lookup_world) <- c("REGION","VAR_ID","UNIT","SCEN1","SCEN2","SCEN3","YEAR","value")

  # Add global aggregate
  out_lookup_world <- subset(out_lookup_world,REGION=="World")
  out_lookup <- rbind(out_lookup,out_lookup_world)

  # Correct naming and format
  out_fao$VAR_ID[which(out_fao$VAR_ID=="Food")] <- "food"
  out_fao$VAR_ID[which(out_fao$VAR_ID=="PROD")] <- "Prod"
  out_fao$ITEM[which(out_fao$ITEM=="ALL")] <- "TOT"
  out_fao <- out_fao %>% filter(!ITEM %in% c("FLAX","CSil"))
  out_globiom$YEAR <- as.integer(levels(out_globiom$YEAR))[out_globiom$YEAR]
  out_lookup$YEAR <- as.integer(levels(out_lookup$YEAR))[out_lookup$YEAR]

  # Extract variables
  vars_lookup <- unique(out_lookup$VAR_ID)
  vars_globiom <- unique(out_globiom$VAR_ID)
  vars_fao <- unique(out_fao$VAR_ID)[-c(12:15)]

  # Filter FAO variables for plotting
  out_globiom <- out_globiom %>% filter(VAR_ID %in% vars_fao & UNIT %in% unique(out_fao$UNIT))

  # Merge GLOBIOM and FAO output files
  out_merged <- rbind(out_globiom,out_fao)

  # Create y labels for lookup table
  units <- unique(str_c(out_lookup$VAR_ID, "  [",out_lookup$UNIT,"]"))

  # Merge scenarios into a single dimension
  out_lookup <- out_lookup %>% mutate(Scenario=str_c(out_lookup$SCEN1,"_",out_lookup$SCEN2,"_",out_lookup$SCEN3))

  # Select scenarios for plotting
  scen_list <- unique(get_mapping()) %>% dplyr::select(-ScenNr)
  scen_for_plot <- scen_list %>% filter(ScenLoop %in% scenarios_for_lookup)
  scen_for_plot <- str_c(scen_for_plot[,1],"_",scen_for_plot[,2],"_",scen_for_plot[,3])
  out_lookup <- out_lookup %>% filter(Scenario %in% scen_for_plot)

  # Correct FAO scenario naming
  out_lookup$Scenario[which(out_lookup$Scenario == "FAO_FAO_FAO")] <- "FAO"

  # Plot items and save to pdf
  pdf(path(plot_dir,"plots_lookup.pdf"), width = 12, height = 14,onefile = T)
  for (i in 1:length(vars_lookup)){
    plot_data <- out_lookup %>% filter(VAR_ID == vars_lookup[i])
    if (dim(plot_data)[1] > 0){
      print(
        ggplot(plot_data,aes(x=YEAR,y=value,col=as.factor(Scenario))) +
          geom_line()  + facet_wrap(~factor(REGION), scales = "free") + theme_light() + labs(x="YEAR",y=units[i],col="Scenario") +
          theme(legend.position = "bottom") + theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
          theme(strip.background =element_rect(fill="white"))+ theme(strip.text = element_text(colour = 'black'))

      )
    }
  }
  dev.off()

  # Merge scenarios into a single dimension
  out_merged <- out_merged %>% mutate(Scenario=str_c(out_merged$SCEN1,"_",out_merged$SCEN2,"_",out_merged$SCEN3))

  # Select scenarios for plotting
  scen_list <- unique(get_mapping()) %>% dplyr::select(-ScenNr)
  scen_for_plot <- scen_list %>% filter(ScenLoop %in% scenarios_for_globiom)
  scen_for_plot <- str_c(scen_for_plot[,1],"_",scen_for_plot[,2],"_",scen_for_plot[,3])
  out_merged <-out_merged %>% filter(Scenario %in% c(scen_for_plot,"FAO_FAO_FAO"))

  # Aggregate regions for global outputs
  world <- aggregate(out_merged["value"],by=out_merged[c("VAR_ID","UNIT","ITEM","SCEN1","SCEN2","SCEN3","YEAR")],FUN="sum")
  world <- world %>% mutate(REGION="World",Scenario=str_c(world$SCEN1,"_",world$SCEN2,"_",world$SCEN3)) %>% relocate(REGION, .after = UNIT)

  # Compute global yields
  world_yld <- world %>% dplyr::select(-UNIT) %>% filter(VAR_ID %in% c("Prod","Area"),Scenario != "FAO_FAO_FAO") %>% spread(VAR_ID,value,convert = F)
  world_yld <- world_yld %>% mutate(VAR_ID = "YILM", UNIT = "t / ha", value=Prod/Area)
  world_yld_fao <- world %>% dplyr::select(-UNIT) %>% filter(VAR_ID %in% c("Prod","Area"),Scenario == "FAO_FAO_FAO") %>% spread(VAR_ID,value,convert = F)
  world_yld_fao <- world_yld_fao %>% mutate(VAR_ID = "YILM", UNIT = "t / ha",value=Prod/Area)
  world_yld <- world_yld %>% dplyr::select(c(-Area,-Prod)) %>% relocate(UNIT, .before = REGION)
  world_yld_fao <- world_yld_fao %>% dplyr::select(c(-Area,-Prod)) %>% relocate(UNIT, .before = REGION)
  world_yld <- world_yld %>% relocate(VAR_ID, .before = UNIT)
  world_yld_fao <- world_yld_fao %>% relocate(VAR_ID, .before = UNIT)
  world <- world %>% filter(VAR_ID != "YILM")

  # Merge global outputs to data table
  out_merged <- rbind(out_merged,world,world_yld,world_yld_fao)

  # Correct FAO scenario naming
  out_merged$Scenario[which(out_merged$Scenario == "FAO_FAO_FAO")] <- "FAO"

  # Remove items not reported for GLOBIOM and filter units
  out_merged <- out_merged %>% filter(! ITEM %in% c("Oats","Rye","Peas","SugB"))

  # Plot items and save to pdf
  for (i in 1:length(vars_fao)){
    pdf(path(plot_dir,str_glue("plots_globiom_",vars_fao[i],".pdf")), width = 14, height = 14,onefile = T)
    plot_data <- out_merged %>% filter(VAR_ID == vars_fao[i])
    items <-  unique(plot_data$ITEM)
    for (j in 1:length(items)){
      print(
      plot_data %>% filter(ITEM==items[j]) %>%
        ggplot(aes(x=YEAR,y=value,col=as.factor(Scenario))) +
          geom_line()  + facet_wrap(~factor(REGION), scales = "free") + theme_light() + labs(x="YEAR",y=items[j],col="Scenario") +
          theme(legend.position = "bottom") + theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
          theme(strip.background =element_rect(fill="white"))+ theme(strip.text = element_text(colour = 'black'))
      )
    }
    dev.off()
  }

}

# Get the return values of job log files, or NA when a job did not terminate normally.
get_return_values <- function(log_directory, log_file_names) {
  return_values <- c()
  return_value_regexp <- "\\(1\\) Normal termination \\(return value (\\d+)\\)"
  for (name in log_file_names) {
    loglines <- readLines(path(log_directory, name))
    return_value <- as.integer(str_match(tail(grep(return_value_regexp, loglines, value=TRUE), 1), return_value_regexp)[2])
    return_values <- c(return_values, return_value)
  }
  return(return_values)
}
