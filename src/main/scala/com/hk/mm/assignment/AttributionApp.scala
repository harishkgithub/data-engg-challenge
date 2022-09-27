package com.hk.mm.assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lag, lit, sum, when}
import org.apache.spark.sql.{Encoders, SparkSession, functions}

object AttributionApp {

  //    ### Events
  //      This dataset contains a series of interactions of users with brands: events.csv
  //    ** Schema :**
  //    Column number | Column name | Type | Description
  //    ------------- | ------------- | ------------- | -------------
  //    1 | timestamp | integer | Unix timestamp when the event happened.
  //    2 | event_id | string(UUIDv4) | Unique ID for the event.
  //    3 | advertiser_id | integer | The advertiser ID that the user interacted with.
  //    4 | user_id | string(UUIDv4) | An anonymous user ID that generated the event.
  //    5 | event_type | string | The type of event.Potential values: click, visit, purchase
  case class Event(timestamp: Int, event_id: String, advertiser_id: Int, user_id: String, event_type: String)


  //      ### Impressions
  //        This dataset contains a series of ads displayed to users online for different advertisers:impressions.csv
  //      ** Schema :**
  //      Column number | Column name | Type | Description
  //      ------------- | ------------- | ------------- | -------------
  //      1 | timestamp | integer | Unix timestamp when the impression was served.
  //      2 | advertiser_id | integer | The advertiser ID that owns the ad that was displayed.
  //      3 | creative_id | integer | The creative (or ad) ID that was displayed.
  //      4 | user_id | string(UUIDv4) | An anonymous user ID this ad was displayed to.
  case class Impression(timestamp: Int, advertiser_id: Int, creative_id: Int, user_id: String)

  def main(args: Array[String]): Unit = {

    // initialise spark session (running in "local" mode)
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("AttributeApp")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)

    println("Attribute analysis")
    val eventsDFCsv = sparkSession
      .read
      .option("header", false)
      .option("inferSchema", true)
      .csv("src/resources/events.csv")
      .toDF(eventColNames: _*)
      .as[Event]

    eventsDFCsv.printSchema()
    eventsDFCsv.show(100, true);

    val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)

    val impressionsDFCsv = sparkSession
      .read
      .option("header", false)
      .option("inferSchema", true)
      .csv("src/resources/impressions.csv")
      .toDF(impressionColNames: _*)
      .as[Impression]

    impressionsDFCsv.printSchema()
    impressionsDFCsv.show(100, true);

    println("impressionsDFCsv count " + impressionsDFCsv.count());

    val maxSessionDuration = 60L
    val eventWithSessionIds = eventsDFCsv
      .select('user_id,'advertiser_id, 'timestamp,
        lag('timestamp, 1)
          .over(Window.partitionBy('user_id).orderBy('timestamp))
          .as('prevTimestamp))
      .select('user_id,'advertiser_id, 'timestamp,
        when('timestamp.minus('prevTimestamp) < lit(maxSessionDuration), lit(0)).otherwise(lit(1))
          .as('isNewSession))
      .select('user_id,'advertiser_id, 'timestamp,
        sum('isNewSession)
          .over(Window.partitionBy('user_id,'advertiser_id).orderBy('user_id,'advertiser_id, 'timestamp))
          .as('sessionId))

    eventWithSessionIds.printSchema();
    eventWithSessionIds.show(100);

//    val ds = eventWithSessionIds
//      .groupBy("user_id", "sessionId")
//      .agg(functions.min("timestamp").as("startTime"),
//        functions.max("timestamp").as("endTime"),
//        count("*").as("count"))
//    ds.printSchema()
//    ds.show(100);

    // terminate underlying spark context
    sparkSession.stop()
  }
}
