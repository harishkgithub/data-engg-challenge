@echo off
e:
cd \
cd E:\harish\MyLab\spark-3.3.0-bin-hadoop3\bin

set SPARH_HOME=E:\harish\MyLab\spark-3.3.0-bin-hadoop3
set SPARK_LIB_PATH=%SPARK_HOME%\jars

set events_path=%1
set impressions_path=%2
set count_of_events=%3
set tmp_count_of_events=%~dp0\tmp_count_of_events

:EXECUTE
echo e:spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] %~dp0\..\..\..\target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar %events_path% %impressions_path% %count_of_events%
echo ......
e:spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] %~dp0\..\..\..\target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar %events_path% %impressions_path% %tmp_count_of_events%
:EXIT
pause