@echo off

set count_of_events=%1
set tmp_count_of_events=%~dp0\tmp_count_of_events
echo copying output files
COPY %tmp_count_of_events%\part* %count_of_events%
echo deleting temporary files
del /f %tmp_count_of_events%\*
rmdir %tmp_count_of_events%

set count_of_users=%2
set tmp_count_of_users=%~dp0\tmp_count_of_users
echo copying output files
COPY %tmp_count_of_users%\part* %count_of_users%
echo deleting temporary files
del /f %tmp_count_of_users%\*
rmdir %tmp_count_of_users%

echo Completed!!!