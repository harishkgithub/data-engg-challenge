@echo off
title Attribute application runner script!

echo Welcome to Attribute application report generator!
echo Navigating to source code
cd ../../../
echo Compiling the project
sbt package
echo "Running Attribution application for report"
start spark-submit --master local[1] .\target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar  file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\events.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\impressions.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events


c:
cd \
cd E:\harish\MyLab\spark-3.3.0-bin-hadoop3

d:
cd \
cd d:\app-to-test-on-spark

set SPARH_HOME=E:\harish\MyLab\spark-3.3.0-bin-hadoop3
set SPARK_LIB_PATH=%SPARK_HOME%\jars

if "%1" == "" (
GOTO READINPUT
) ELSE (
set name=%1
GOTO EXECUTE
)

:READINPUT
set /p name=Please provide name:

:EXECUTE
echo Executing %name%
c:spark-submit --class %name% --jars %LIBRARY_TO_INCLUDE% --master local ./%name%.jar

:EXIT
pause
