# data-engg-challenge
Coding challenge with Apache spark, scala

# Windows users :- 
Prerequisite:

i) Java 8 or above is installed
Set JAVA_HOME = C:\Program Files\Java\jdk1.8.0_201
Set PATH = %PATH%;%JAVA_HOME%

ii) Spark 3.3.0 is installed
Download spark from https://spark.apache.org/downloads.html
I used Spark 3.3.0 with "Pre-built for Apache Hadoop 3.3 and later" [spark-3.3.0-bin-hadoop3.tgz was unzipped using 7z tool]
"E:\harish\MyLab\spark-3.3.0-bin-hadoop3" is unzipped spark location which contains bin folder having spark-submit script

Ensure spark-submit is available to use at all directories
Set SPARK_HOME  = E:\harish\MyLab\spark-3.3.0-bin-hadoop3
Set PATH = %PATH%;%SBT_HOME%\bin

iii) sbt (scala build tool) is installed
I used the installer from https://github.com/sbt/sbt/releases/download/v1.7.1/sbt-1.7.1.msi

Ensure sbt is available to use at all directories
Set SBT_HOME = E:\harish\MyLab\sbt\
Set PATH = %PATH%;%SBT_HOME%\bin

iv) winutils.exe is downloaded
Clone https://github.com/steveloughran/winutils
Set HADOOP_HOME =  E:\harish\MyLab\Idea-workspace\winutils\hadoop-2.8.3\bin
Run 
%HADOOP_HOME%bin\winutils.exe chmod 755 E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources
"E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources" is the folder where we write our output files


Step 1: Clone the project
git clone https://github.com/harishkgithub/data-engg-challenge.git
or 
Download the project from https://github.com/harishkgithub/data-engg-challenge

Step 2: Building the project 
i) Navigate inside the project folder "data-engg-challenge"
cd data-engg-challenge

ii) Switch to branch dev_3.3.0_hadoop3
git checkout dev_3.3.0_hadoop3

iii) Run "sbt package"
this will generate the jar "target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar"

Step 3: Running the script
Navigate to script folder "data-engg-challenge\src\main\Scripts"
cd src\main\Scripts

Run the script to run the spark submit command on input files
RunAttribute.bat file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\events.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\impressions.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_users.csv

Here
i)events.csv path is file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\events.csv
ii)impressions.csv file path is file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\impressions.csv
iii)count_of_events.csv output file path is E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events.csv
iii)count_of_users.csv output file path is E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_users.csv

Once the spark submit command completes successfully again Navigate to script folder "data-engg-challenge\src\main\Scripts"
Run the script to rename the intermediate output files to required format
PostProcessing.bat E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_users.csv

Press 'Y' if prompted for all input
On success we will see output files at
E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events.csv 
E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_users.csv

# Tested on
sbt 1.7.1 
Java 1.8.0_341
Windows 10
Spark 3.3.0
Hadoop 2.8.3
Intellij 2022.2.2
WIndows command prompt

# Mac
 sh RunAttribution.sh /Users/harishkumar/EsKaZo/hk/data-engg-challenge-dev_3.3.0_hadoop3/src/resources/data/events.csv \
 /Users/harishkumar/EsKaZo/hk/data-engg-challenge-dev_3.3.0_hadoop3/src/resources/data/impressions.csv \
 /Users/harishkumar/EsKaZo/hk/data-engg-challenge-dev_3.3.0_hadoop3/src/resources/data/count_of_events.csv \
 /Users/harishkumar/EsKaZo/hk/data-engg-challenge-dev_3.3.0_hadoop3/src/resources/data/count_of_uniqueusers.csv

# Additional context:
Test run :2
RunAttribute.bat E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\impressions.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_users.csv

PostProcessing.bat E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_users.csv
