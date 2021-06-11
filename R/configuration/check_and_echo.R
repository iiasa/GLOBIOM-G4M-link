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

# 3. Echo
for (name in config_names)  {
  print(str_glue("{name} = ", get(name)))
}
