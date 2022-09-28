package com.hk.mm.assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, countDistinct, first, lag, lit, max, rank, sum, when, window}
import org.apache.spark.sql.types.{IntegerType, LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession, functions}

import scala.util.Try

/**
 * The statistics that the application should compute are:
 * The count of attributed events for each advertiser, grouped by event type.
 * The count of unique users that have generated attributed events for each advertiser , grouped by event type.
 */
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

  def displayData(df: Dataset[AnyRef], size: Int = 100, truncate: Boolean = false): Unit = {
    df.printSchema()
    df.show(size, truncate)
  }

  def main(args: Array[String]): Unit = {
    try {
      var debugMode = true
      val trueValue = true
      val falseValue = false
      var eventsPath = "src/resources/events.csv"
      var impressionsPath = "src/resources/impressions.csv"

      if (args != null && args.length >= 2) {
        eventsPath = args(0)
        impressionsPath = args(1)
        if (args.length >= 3) {
          debugMode = Try(args(2).toBoolean).getOrElse(false)
        }
      }
      println(s"Reading events from, $eventsPath")
      println(s"Reading Impressions from, $impressionsPath")

      // initialise spark session (running in "local" mode)
      val sparkSession = SparkSession.builder
        .master("local")
        .appName("AttributeApp")
        .getOrCreate()

      sparkSession.sparkContext.setLogLevel("ERROR")
      import sparkSession.implicits._

      val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)

      println("----------------- Attribute analysis --------------------------")
      println("Reading events ....")
      val eventsDS = sparkSession
        .read
        .option("header", falseValue)
        .option("inferSchema", trueValue)
        .csv(eventsPath)
        .toDF(eventColNames: _*)
        .as[Event]

      if (debugMode) {
        eventsDS.printSchema()
        println(s"Events count : ${eventsDS.count()}")
        eventsDS.show(false)
      }

      println("Deduplication of events within minute to avoid click on an ad twice by mistake.")
      // Create an instance of UDAF DeDupMultiEvent.
      val gm = new DeDupMultiEvent

      val partWindowCondn = Window.partitionBy('user_id, 'advertiser_id, 'event_type)
        .orderBy("timestamp")

      val eventsAfterDedupDF = eventsDS
        .select('timestamp, 'event_id, 'advertiser_id, 'user_id, 'event_type,
          gm('timestamp).over(partWindowCondn).as("prev_min_time_in_minute"))
        .filter('timestamp === 'prev_min_time_in_minute)
        .drop('prev_min_time_in_minute)

      if (debugMode) {
        eventsAfterDedupDF.printSchema()
        println(s"De-duplicated events count : ${eventsAfterDedupDF.count()}")
        eventsAfterDedupDF.show(100, falseValue)
      }
      println("Deduplication phase complete.")


      println("Reading impressions ....")
      val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)

      val impressionsDS = sparkSession
        .read
        .option("header", falseValue)
        .option("inferSchema", trueValue)
        .csv(impressionsPath)
        .toDF(impressionColNames: _*)
        .as[Impression]

      if (debugMode) {
        impressionsDS.printSchema()
        println(s"impressions count : ${impressionsDS.count()}")
        impressionsDS.show(100, trueValue)
      }

      //        - The count of attributed events for each advertiser, grouped by event type.

      val eventsAndImpressionDF = eventsAfterDedupDF
        .select(lit(0).as("Type"), 'timestamp, 'advertiser_id, 'user_id,'event_type)
        .union(impressionsDS.select(lit(1).as("Type"), 'timestamp, 'advertiser_id,
          'user_id,lit("NA").as('event_type)))

      if (debugMode) {
        eventsAndImpressionDF.printSchema()
        println(s"Events and impressions combined count : ${eventsAndImpressionDF.count()}")
        eventsAndImpressionDF.show(100, falseValue)
      }

      val markAttributeEventsDF =
        eventsAndImpressionDF
          .select('Type, 'timestamp, 'advertiser_id, 'user_id,'event_type,
            max('Type).over(Window.partitionBy('user_id, 'advertiser_id)
              .orderBy("timestamp")).as("impression_occurred"))
          .filter(('Type === 0).and('impression_occurred === 1))

      if (debugMode) {
        markAttributeEventsDF.printSchema()
        println(s"Marked attribute events count : ${markAttributeEventsDF.count()}")
        markAttributeEventsDF.show(100, trueValue)
      }

      val attributedEventsByAdvertiserDF = markAttributeEventsDF
        .groupBy('advertiser_id, 'user_id,'event_type)
        .agg(max('impression_occurred))
        .groupBy('advertiser_id,'event_type)
        .agg(count('advertiser_id).as("count_of_events"))

      if (debugMode) {
        attributedEventsByAdvertiserDF.printSchema()
        println(s" The count of attributed events dataset count : ${attributedEventsByAdvertiserDF.count()}")
        attributedEventsByAdvertiserDF.show(100, trueValue)
      }

      println("----------------- Attribute analysis completed --------------------------")
      sparkSession.stop()
    } catch {
      case genEx: Exception =>
        println("Exception while running application.")
        genEx.printStackTrace()

    }
  }
}
