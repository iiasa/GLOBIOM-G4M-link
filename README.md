# GLOBIOM-G4M-link

Automatize the link between GLOBIOM and G4M. The stages of the link are documented and coded in the R notebook `GLOBIOM-G4M-link.Rmd`. To preview the notebook and its possible output, visit [this page](https://iiasa.github.io/GLOBIOM-G4M-link/GLOBIOM-G4M-link.nb.html) or load `GLOBIOM-G4M-link.nb.html` into a web browser.

- [Getting set](#getting-set)
- [R Dependencies](#r-dependencies)
- [Running the notebook](#running-the-notebook)

## Getting set

1. Clone the repository to a working directory able to hold some gigabytes. If you use the
   Git command line client use  
   `git clone --recursive <repository URL>`  
   to also clone the `Condor_run_R` and `DownScale` submodules. You can access the repository
   URL and other clone options by clicking on the 'Code' drop-down menu at the top right of the
   [GitHub repository page](https://github.com/iiasa/GLOBIOM-G4M-link).
2. Check that the `Condor_run_R` and `DownScale` subdirectories have content. If not, the
   clone did not clone the Git submodule that lives there. To still get it, with the command
   line Git client do  
   `git submodule update --init --recursive`  
   from the root of the cloned repository.
3. Checkout a GLOBIOM branch from subversion by running the `checkout.bat` script from
   the command prompt or your bash shell. This requires the `svn` Subversion command line
   client to be accessible via your PATH environment variable (on-path). You may wish
   to change the Subversion URL in `checkout.bat` to check out a different branch. The
   branch should be close to the Trunk version of GLOBIOM.
   
   If you don't have `svn` on-path, perform the `svn` step in `checkout.bat` with a
   graphical client like TortoiseSVN, making sure the working copy goes into a `GLOBIOM`
   subdirectory of the root level of your clone of this repository. Also, make a `Condor`
   subdirectory of `GLOBIOM`.
4. Run the GLOBIOM precompilation `GLOBIOM/Data/0_executebatch_total.gms` and thereafter
   the model `GLOBIOM/Model/0_executebatch.gms` up to the scenarios stage 6 (comment
   out the stages >= 6). This will provide a restart file in the `GLOBIOM/Model/t`
   directory that is used by the notebook to perform parallel scenario (stage 6)
   runs on the Limpopo cluster.
6. Install the R dependencies if needed.

## R Dependencies

The notebook depends on:
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

## Running the notebook

Double click on `GLOBIOM-G4M-link.Rproj` to open the project in RStudio. In the 'Files' tab, double click `GLOBIOM-G4M-link.Rmd` to open the notebook. Read the instructions contained in the notebook, and run its chunks. Typically, you will run the chunks in first-to-last order, but if a chunk fails, you may need to re-run it.

When you run the notebook, the output HTML file (`GLOBIOM-G4M-link.nb.html`) will be updated.
