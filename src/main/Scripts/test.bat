@echo off
e:
cd \
cd E:\harish\MyLab\spark-3.3.0-bin-hadoop3\bin

set SPARH_HOME=E:\harish\MyLab\spark-3.3.0-bin-hadoop3
set SPARK_LIB_PATH=%SPARK_HOME%\jars

if "%1" == "" (
GOTO READINPUT
) ELSE (
set events_path=%1
GOTO EXECUTE
)

:READINPUT
set /p events_path=Please enter events.csv path:

if "%2" == "" (
GOTO READINPUT
) ELSE (
set impressions_path=%2
GOTO EXECUTE
)

:READINPUT
set /p impressions_path=Please enter impressions.csv path:

if "%3" == "" (
GOTO READINPUT
) ELSE (
set count_of_events=%3
GOTO EXECUTE
)

:READINPUT
set /p count_of_events=Please enter output count of_events.csv path:


:EXECUTE
e:spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] %~dp0\..\..\..\target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar %events_path% %impressions_path% %count_of_events%
echo "completed!!!"
:EXIT
pause
