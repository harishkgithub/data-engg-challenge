#!/bin/sh

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
  tmp_count_of_events=./tmp_count_of_events
  tmp_count_of_users=./tmp_count_of_users
fi

echo "Simple attribution application that produces a report!!"
echo $JAVA_HOME
JAVA_HOME="/C/Users/kumard/.jdks/corretto-1.8.0_342"

echo spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] \
../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar $events_path $impressions_path $tmp_count_of_events $tmp_count_of_users

echo

spark-submit --class com.hk.mm.assignment.AttributionApp --master local[1] \
../../../target/scala-2.12/data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar $events_path $impressions_path $tmp_count_of_events $tmp_count_of_users

echo

echo "Copying output files of count_of_events"
cat $tmp_count_of_events/part*.csv $count_of_events
echo "deleting temporary files of count_of_events"
rm -r $tmp_count_of_events

echo "Copying output files of count_of_users"
cat $tmp_count_of_users/part*.csv count_of_users
echo "deleting temporary files of count_of_users"
rm -r $tmp_count_of_users

echo "Analysis completed!!!"
