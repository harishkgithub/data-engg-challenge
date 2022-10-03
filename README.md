# data-engg-challenge
Coding challenge with Apache spark, scala

# Linux/Mac users :-
## Prerequisite installation :
i) Java 8 or above should be installed
Set JAVA_HOME and PATH 
```
export JAVA_HOME=<Path to jdk folder>
export PATH=$PATH:$JAVA_HOME
```

ii) Spark 3.3.0 is installed

Download spark from https://spark.apache.org/downloads.html
I used Spark 3.3.0 with "Pre-built for Apache Hadoop 3.3 and later" [spark-3.3.0-bin-hadoop3.tgz unzip it]
"/Users/harishkumar/spark-3.3.0-bin-hadoop3/" is unzipped spark location which contains bin folder having spark-submit script

Set SPARK_HOME and PATH
```
export SPARK_HOME=/Users/harishkumar/spark-3.3.0-bin-hadoop3/
export PATH=$PATH:$SPARK_HOME/bin
```

iii) sbt (scala build tool) is installed
```
brew install sbt
```


## Deploy and run script:

Step 1: Download the source code from email attachment "data-engg-challenge-main.zip" and unzip it.
Open terminal and give write permissions for unzipped folder "/Users/harishkumar/data-engg-challenge-main" 
navigate inside the unzipped folder "data-engg-challenge-main" to the Scripts directory 
Ex:
```
chmod -R 755 /Users/harishkumar/data-engg-challenge-main/

cd /Users/harishkumar/data-engg-challenge-main/src/main/Scripts/Linux_Mac
```

Step 2: Building and running the project.
i)Run the below script to build the jar [Here we check for java,sbt,spark installations and build the project to runnable jar]
```
sh deployAttribution.sh
```

then run the below command to execute the application
```
sh RunAttribution.sh /Users/harishkumar/data-engg-challenge-main/src/resources/data/events.csv  /Users/harishkumar/data-engg-challenge-main/src/resources/data/impressions.csv  \
/Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_events.csv  /Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_uniqueusers.csv
```

Once the script completes we will have our output files of count_of_events.csv and count_of_uniqueusers.csv store at
```
/Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_events.csv  and 
/Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_uniqueusers.csv
```

Note: You can give your own events.csv and impressions.csv path and output location
Here in my example 

i)/Users/harishkumar/data-engg-challenge-main/src/resources/data/events.csv  is events.csv path

ii)/Users/harishkumar/data-engg-challenge-main/src/resources/data/impressions.csv is impressions.csv file path

iii)/Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_events.csv  is count_of_events.csv output file path

iii)/Users/harishkumar/data-engg-challenge-main/src/resources/data/output/count_of_uniqueusers.csv is count_of_users.csv output file path

# Tested on
sbt 1.7.1
Java 1.8.0_341
Spark 3.3.0
Hadoop 2.8.3
Intellij 2022.2.2
MacOS Monterey

====================================================================================================================


















 
# Windows users (In-Complete)  :- 
 We can use this guide to run the project on windows but this not tested and its incomplete

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


Step 1: Step 1: Download the source code from email attachment "data-engg-challenge-main.zip" and unzip it.
Open terminal and give write permissions for unzipped folder

Step 2: Building the project 
i) Navigate inside the project folder "data-engg-challenge"
cd data-engg-challenge

ii) Run "sbt package"
this will generate the jar "target\scala-2.12\data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar"

Step 3: Running the script
Navigate to script folder "data-engg-challenge\src\main\Scripts\windows"
cd src\main\Scripts\windows

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

====================================================================================================================