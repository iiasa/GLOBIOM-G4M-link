# GLOBIOM-G4M-link

R script to automatize the link between GLOBIOM and G4M.

## Getting started

1. Clone this repository to a working directory able to hold some gigabytes. For example
   somewhere on your H: drive.
3. Check the DownScale subdirectory has content. If not, the clone did not bring in the
   Git submodule that lives there. To still get it, with the command line Git client do  
   `git submodule update --init --recursive`  
   from the root of the cloned repository.

## Dependencies

The script depends on:
- The [tidyverse](https://www.tidyverse.org/) curated R package collection.
- [**gdxrrw**](https://github.com/GAMS-dev/gdxrrw), an R package for
  reading/writing GDX files from R. For a list of which binary package versions
  match what R versions, see the [**gdxrrw** wiki](https://github.com/GAMS-dev/gdxrrw/wiki).
  * **Beware**, as of version V1.0.8, **gdxrrw** requires GAMS >= V33.
    When you use an earlier GAMS version, use an earlier **gdxrrw** version.
  * Note that if you can compile packages, for example with [Rtools](https://cran.r-project.org/bin/windows/Rtools/),
    any source package version can be made to work with your R version.
  * If you don't want to go through the hassle of installing Rtools, try a binary
    package built for a slightly earlier R release than the one you have installed.
    A package built for R version x.y.a may work with R version x.y.b (where x, y, a,
    and b are digits and a < b), though possibly with some warnings.
- [**gdxtools**](https://github.com/lolow/gdxtools).
