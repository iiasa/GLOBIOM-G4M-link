:<<BATCH
@echo off
pushd %~dp0
svn checkout svn://hpg909/svn_globiom/globiom_live/Branches/Branch_trunk_EPA GLOBIOM
mkdir GLOBIOM\Condor
popd
exit /b :: end batch script processing
BATCH
# Platform-agnostic script.
# Put batch commands above and functionally-identical Linux/MacOS shell commands
# terminated by a # below. When saving this script with CR+LF end-of-line breaks,
# the trailing # makes the shell ignore the CR. Must be run with the bash shell.
pushd "$(dirname "$0")" #
set -e #
svn checkout svn://hpg909/svn_globiom/globiom_live/Branches/Branch_trunk_EPA GLOBIOM #
mkdir GLOBIOM/Condor #
popd #
