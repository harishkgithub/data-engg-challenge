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

==================================================================================
