:<<BATCH
@echo off
pushd %~dp0
if not "%~1"=="" goto continue
echo "This batch file requires a Trunk commit/revision number as only argument."
exit /b 1
:continue
echo Checking out commit %~1 from the GLOBIOM Trunk
echo win svn checkout --revision %~1 svn://hpg909/svn_globiom/globiom_live/Trunk GLOBIOM
popd
exit /b :: end batch script processing
BATCH
# Platform-agnostic script.
# Put batch commands above and functionally-identical Linux/MacOS shell commands
# terminated by a # below. When saving this script with CR+LF end-of-line breaks,
# the trailing # makes the shell ignore the CR. Must be run with the bash shell.
pushd "$(dirname "$0")" #
if [ -z "$1" ] #
  then #
    echo "This batch file requires a Trunk commit/revision number as only argument." #
    exit 1 #
fi #
set -e #
echo sh svn checkout --revision $1 svn://hpg909/svn_globiom/globiom_live/Trunk GLOBIOM #
popd #
