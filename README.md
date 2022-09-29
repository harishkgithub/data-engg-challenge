# data-engg-challenge
coding challenge with Apache spark, scala

spark-submit --master local[1] data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\events.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\impressions.csv file:///E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\count_of_events

# Windows users
Prerequisite:
i) Java 8 or above is installed
ii) Spark 3.3.0 is installed 
iii) sbt (scala build tool) is installed
iv) winutils.exe is downloaded

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

Press 'Y' if prompted for input

# Tested on
sbt 1.7.1 
Java 1.8.0_341
Windows 10


# Additional context:
Test run :2
RunAttribute.bat E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\impressions.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_users.csv

PostProcessing.bat E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_events.csv E:\harish\MyLab\Idea-workspace\data-engg-challenge_mm\src\resources\data\count_of_users.csv