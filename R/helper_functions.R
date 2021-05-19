

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
merge_gdx <- function(PROJECT,WD,c_nr){
  
  prior_wd <- getwd()
  # Set wd to gdx folder
  
  setwd(WD)
  
  merge_args <- c()
  merge_args <- c(merge_args, str_glue("output_",PROJECT,"_",c_nr,".*.gdx"))
  merge_args <- c(merge_args, str_glue("output=output_",PROJECT,"_",c_nr,"_merged.gdx"))
  
  # Invoke GDX merge
  
  error_code <- tryCatch(
    system2("gdxmerge", args=merge_args),
    error=function(e) e
  )
  
  if(error_code != 0){
    setwd(prior_wd)
    stop("Bad return from gams")
  }
  
  setwd(prior_wd)
  
}


# Merge gdx files from downscaling according to the scenario number
merge_gdx_down <- function(PROJECT,wd_out,s_list,s_cnt,c_nr,path_out){
  prior_wd <- getwd()
  
  setwd(wd_out)
  s_list <-  sprintf("%06d", s_list)
  merge_args <- c()
  merge_args <- c(merge_args, str_c("downscaled_",PROJECT,"_",c_nr,".",s_list,".gdx"))
  merge_args <- c(merge_args, str_glue(str_glue("output=",path_out,"output_landcover_",PROJECT,"_",s_cnt,"_merged.gdx")))

  # Invoke GDX merge
  
  error_code <- tryCatch(
    system2("gdxmerge", args=merge_args),
    error=function(e) e
  )
  
  if(error_code != 0){
    setwd(prior_wd)
    stop("Bad return from gams")
  }
  
  setwd(prior_wd)
}




