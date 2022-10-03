#!/bin/sh

if [ -z "${JAVA_HOME}" ]; then
  java_not_installed=$( java --version 2>&1 | grep -i "not found" | wc -l)
  if [ $java_not_installed -ge 1 ] ; then
    echo "ERROR: Either JAVA_HOME is not set or Java is not installed. Please install java and set JAVA_HOME and set it to PATH"
    exit 1
  fi
fi

echo
echo "Checking java version :"
java --version
echo
sbt_not_installed=$( sbt --version 2>&1 | grep -i "not found" | wc -l)
if [ $sbt_not_installed -ge 1 ] ; then
  echo "ERROR: Sbt (Scala build tool) is missing.we can not compile our project. Please install sbt"
  exit 1
fi

echo "Checking sbt version :"
sbt --version
echo

if [ -z "${SPARK_HOME}" ]; then
  spark_not_installed=$( spark-submit --version 2>&1 | grep -i "not found" | wc -l)
  if [ $spark_not_installed -ge 1 ] ; then
    echo "ERROR: Either SPARK_HOME is not set or Apache spark is not installed. Please install Apache spark and set SPARK_HOME and set it to PATH"
    exit 1
  fi
  echo "SPARK_HOME is not set. Please set it and try again"
  exit 1
fi

echo "Checking Apache spark version :"
spark-submit --version
echo

echo "Navigating to source code"
cd ../../../../
pwd
echo
echo "Compiling source code"
sbt package
echo

echo "Navigating back to runner script"
cd src/main/Scripts/Linux_Mac/
pwd
echo "Please run the application wit RunAttribution.sh :
sh RunAttribution.sh /Users/harishkumar/data-engg-challenge-main/src/resources/data/events.csv  \
/Users/harishkumar/data-engg-challenge-main/src/resources/data/impressions.csv  \
/Users/harishkumar/data-engg-challenge-main/src/resources/data/outputev \
/Users/harishkumar/data-engg-challenge-main/src/resources/data/outputusr
"

echo