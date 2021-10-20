# 1. Check configuration settings against the names and types of the default
#    configuration parameters as captured in config_names and config_types.
# 2. Perform setting-specific checks.
# 3. Echo the configuration settings when all is well.

# 1. Check
for (i in seq_along(config_names))  {
  name <- config_names[i]
  if (!exists(name)) stop(str_glue("Mandatory config setting {name} is not set in config file {args[1]}!"))
  type <- typeof(get(name))
  if (type != config_types[[i]] &&
      type != "integer" && # R has no stable numerical type
      type !=  "double" && # R has no stable numerical type
      type != "NULL" && # allow for configured vector being empty
      config_types[[i]] != "NULL" # allow for default vector being empty
  ) stop(str_glue("{name} set to wrong type in {configuration_file}, type should be {config_types[[i]]}"))
}

# 2. Setting-specific checks

# Check paths
if (!dir_exists(WD_GLOBIOM)) stop(str_glue('Directory WD_GLOBIOM = "{WD_GLOBIOM}" does not exist!'))
if (!dir_exists(WD_DOWNSCALING)) stop(str_glue('Directory WD_DOWNSCALING = "{WD_DOWNSCALING}" does not exist!'))
if (!dir_exists(WD_G4M)) stop(str_glue('Directory WD_G4M = "{WD_G4M}" does not exist!'))
if (!dir_exists(PATH_FOR_G4M)) {
  if (!dir_exists(path_dir(PATH_FOR_G4M))) # stop(str_glue('Neither directory PATH_FOR_G4M = "{PATH_FOR_G4M}" nor its parent directory exist!'))
  dir_create(PATH_FOR_G4M)
  print(str_glue('Creating directory PATH_FOR_G4M = "{PATH_FOR_G4M}"'))
}
if (!dir_exists(WD_G4M)) stop(str_glue('Directory WD_G4M = "{WD_G4M}" does not exist!'))

# Map downscaling types to script file names
ds_type_map <- list(
  default        = "1_downscaling.gms",
  econometric    = "1_downscalingEconometric.gms",
  econometricMNL = "1_downscalingEconometricMNL.gms"
)
if (!(DOWNSCALING_TYPE %in% names(ds_type_map))) {
  stop(str_glue("Invalid DOWNSCALING_TYPE '{DOWNSCALING_TYPE}'! Allowed values: {str_c(names(ds_type_map), collapse=', ')}."))
}
DOWNSCALING_SCRIPT <- path(ds_type_map[DOWNSCALING_TYPE])
rm(ds_type_map)

# Check consistency among scenarios
if (any(!SCENARIOS_FOR_DOWNSCALING %in% SCENARIOS)) stop("Downscaling scenario list must be equal or a subset of the GLOBIOM scenario list")
if (any(!SCENARIOS_FOR_G4M %in% SCENARIOS_FOR_DOWNSCALING)) stop("G4M scenario list must be equal or a subset of the Downscaling scenario list")

# 3. Echos
for (name in config_names)  {
  print(str_glue("{name} = ", toString(get(name))))
}
