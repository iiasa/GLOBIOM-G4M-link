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
scen_list <- eval(parse(text=SCENARIOS))
downs_list <- eval(parse(text=SCENARIOS_FOR_DOWNSCALING))
g4m_list <- eval(parse(text=SCENARIOS_FOR_G4M))

if (any(!downs_list %in% scen_list)) stop("Downscaling scenario list must be equal or a subset of the GLOBIOM scenario list")
if (any(!g4m_list %in% downs_list)) stop("G4M scenario list must be equal or a subset of the Downscaling scenario list")

# 3. Echos
for (name in config_names)  {
  print(str_glue("{name} = ", get(name)))
}
