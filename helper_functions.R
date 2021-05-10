

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
merge_gdx <- function(project,wd,c_nr){
  
  prior_wd <- getwd()
  # Set wd to gdx folder
  
  setwd(wd)
  
  merge_args <- c()
  merge_args <- c(merge_args, str_glue(paste("output_",project,"_",c_nr,".*.gdx",sep="")))
  merge_args <- c(merge_args, str_glue(paste("output=output_",project,"_",c_nr,"_merged.gdx",sep="")))
  
  # Invoke GDX merge
  
  error_code <- tryCatch(
    system2("gdxmerge", args=merge_args),
    error=function(e) e
  )
  
  if(error_code != 0){
    setwd(prior_wd)
    stop(paste("Bad return from gams"))
  }
  
  setwd(prior_wd)
  
}


# Merge gdx files from downscaling 
merge_gdx_down <- function(project,wd_out,s_list,s_cnt,c_nr,path_out){
  prior_wd <- getwd()
  
  setwd(wd_out)
  s_list <-  sprintf("%06d", s_list)
  merge_args <- c()
  merge_args <- c(merge_args, paste0("downscaled_",project,"_",c_nr,".",s_list,".gdx"))
  merge_args <- c(merge_args, str_glue(paste0("output=",path_out,"output_",project,"_",s_cnt,"_merged.gdx")))

  # Invoke GDX merge
  
  error_code <- tryCatch(
    system2("gdxmerge", args=merge_args),
    error=function(e) e
  )
  
  if(error_code != 0){
    setwd(prior_wd)
    stop(paste("Bad return from gams"))
  }
  
  setwd(prior_wd)
}




