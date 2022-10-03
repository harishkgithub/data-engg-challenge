#!/bin/sh
#set -x
usage()
{
  echo "Usage: $0 <events_path> <impressions_path> <count_of_events_path> <count_of_users_path>"
  exit 1
}
if [ $# -lt 4 ] ; then
  usage
else
  events_path=$1
  impressions_path=$2
  count_of_events=$3
  count_of_users=$4
  if [ $# -gt 4 ] ; then
    spark_conf_file=$5
  fi
  tmp_count_of_events=./tmp_count_of_events
  tmp_count_of_users=./tmp_count_of_users
fi

# Check of application jar
if [ -e ../../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar ]
then
   echo "Application jar exists : ../../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar"
else
    echo "ERROR: missing ../../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar can not do analysis."
    echo "ERROR: Please deploy the application using sh deployAttribution.sh first"
    exit 1
fi

echo "Simple attribution application that produces a report!!"

echo spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] \
../../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar \
$events_path $impressions_path $tmp_count_of_events $tmp_count_of_users
echo

spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] \
../../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar \
$events_path $impressions_path $tmp_count_of_events $tmp_count_of_users $spark_conf_file
echo

if [ -e $tmp_count_of_events ]
then
    echo "Copying output files of count_of_events to $count_of_events"
    cat $tmp_count_of_events/part*.csv > $count_of_events
    echo "deleting temporary files of count_of_events"
    rm -r $tmp_count_of_events
else
    echo "ERROR: count_of_events output files missing some issue with spark-submit.Check logs of your spark job"
fi


if [ -e $tmp_count_of_users ]
then
   echo "Copying output files of count_of_unique_users to $count_of_users"
   cat $tmp_count_of_users/part*.csv > $count_of_users
   echo "deleting temporary files of count_of_users"
   rm -r $tmp_count_of_users
   echo "Analysis completed!!!"
else
    echo "ERROR: count_of_unique_users output files missing some issue with spark-submit.Check logs of spark job"
    echo "Analysis failed!!!"
fi





