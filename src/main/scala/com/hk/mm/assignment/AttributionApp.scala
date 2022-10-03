package com.hk.mm.assignment

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, lit, max}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties
import scala.util.Try
import scala.io.Source

/**
 * The statistics that the application should compute are:
 * The count of attributed events for each advertiser, grouped by event type.
 * The count of unique users that have generated attributed events for each advertiser , grouped by event type.
 */
object AttributionApp extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val trueValue = true
  val falseValue = false
  var debugMode = false

  var sparkConfFilePath = ""

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

  class MMInvalidParameterException(s: String) extends Exception(s) {}

  //  def genericExec[T](query: String)(implicit rconv: GetResult[T]) : List[T] = ...

  def displayData[T](df: Dataset[T],
                     desc: String,
                     size: Int = 100,
                     truncate: Boolean = false): Unit = {
    if (debugMode) {
      df.printSchema()
      logger.info(s"$desc: ${df.count()}")
      df.show(size, truncate)
    }
  }

  def usage(errorMsg: String = ""): Unit = {
    logger.info(errorMsg)
    logger.info("Usage : spark-submit --master local[1] data-engg-challenge_2.12-0.1.0-SNAPSHOT.jar <Path to event.csv> <Path to impression.csv> <Path to output count events> Optional<spark conf file>")
  }

  def loadEventsDS(sparkSession: SparkSession,
                   eventsPath: String,
                   eventColNames: Array[String]): Dataset[Event] = {
    import sparkSession.implicits._
    sparkSession.read.option("header", falseValue)
      .option("inferSchema", trueValue)
      .csv(eventsPath)
      .toDF(eventColNames: _*)
      .as[Event]
  }

  def loadImpressionsDS(sparkSession: SparkSession,
                        impressionsPath: String,
                        impressionColNames: Array[String]): Dataset[Impression] = {
    import sparkSession.implicits._
    sparkSession
      .read
      .option("header", falseValue)
      .option("inferSchema", trueValue)
      .csv(impressionsPath)
      .toDF(impressionColNames: _*)
      .as[Impression]
  }

  def dedupEventsDS(sparkSession: SparkSession,
                    eventsDS: Dataset[Event]): DataFrame = {
    import sparkSession.implicits._
    // Create an instance of UDAF DeDupMultiEvent.
    val gm = new DeDupMultiEvent

    val partWindowCondn = Window.partitionBy('user_id, 'advertiser_id, 'event_type)
      .orderBy("timestamp")

    eventsDS
      .select('timestamp, 'event_id, 'advertiser_id, 'user_id, 'event_type,
        gm('timestamp).over(partWindowCondn).as("prev_min_time_in_minute"))
      .filter('timestamp === 'prev_min_time_in_minute)
      .drop('prev_min_time_in_minute)
  }

  def fetchAttributeEvents(sparkSession: SparkSession,
                           eventsAfterDedupDF: DataFrame,
                           impressionsDS: Dataset[Impression]): Dataset[Row] = {
    import sparkSession.implicits._
    val eventsAndImpressionDF = eventsAfterDedupDF
      .select(lit(0).as("Type"), 'timestamp, 'advertiser_id, 'user_id, 'event_type)
      .union(impressionsDS.select(lit(1).as("Type"), 'timestamp, 'advertiser_id,
        'user_id, lit("NA").as('event_type)))

    if (debugMode) {
      eventsAndImpressionDF.printSchema()
      println(s"Events and impressions combined count : ${eventsAndImpressionDF.count()}")
      eventsAndImpressionDF.show(100, falseValue)
    }

    println("Marking/identifying attributed events")
    /* Mark events which are attributed i.e.,
    *  - Attributed event: an event that happened chronologically after an impression and is considered to be
    * the result of that impression. The advertiser and the user of both the impression and the event
    * have to be the same for the event to be attributable. Example: a user buying an object after
    * seeing an ad from an advertiser.*/
    eventsAndImpressionDF
      .select('Type, 'timestamp, 'advertiser_id, 'user_id, 'event_type,
        max('Type).over(Window.partitionBy('user_id, 'advertiser_id)
          .orderBy("timestamp")).as("impression_occurred"))
      .filter(('Type === 0).and('impression_occurred === 1))

  }

  def calculateCountOfEvents(sparkSession: SparkSession,
                             markAttributeEventsDF: Dataset[Row]): DataFrame = {
    import sparkSession.implicits._
    markAttributeEventsDF
      .groupBy('advertiser_id, 'event_type)
      .agg(count('advertiser_id).as("count"))
  }

  def calculateCountOfUniqueEvents(sparkSession: SparkSession,
                                   markAttributeEventsDF: Dataset[Row]): DataFrame = {
    import sparkSession.implicits._
    markAttributeEventsDF
      .groupBy('advertiser_id, 'user_id, 'event_type)
      .agg(lit(1).as("unique_user_event_type"))
      .groupBy('advertiser_id, 'event_type)
      .agg(count('unique_user_event_type).as("count"))

  }

  def storeDataToCsv(attributedEventsByAdvertiserDF: DataFrame,
                     saveMode: SaveMode,
                     outputFilePath: String): Boolean = {
    try {
      attributedEventsByAdvertiserDF
        .write
        .mode(saveMode)
        .option("header", trueValue)
        .csv(outputFilePath)
      println(s"Successfully stored data to $outputFilePath")
      true
    } catch {
      case genEx: Exception =>
        println(s"Exception while storing data to $outputFilePath")
        genEx.printStackTrace()
        false
    }
  }

  def main(args: Array[String]): Unit = {
    try {

      var eventsPath = ""
      var impressionsPath = ""
      var countOfEventsOutputPath = ""
      var countOfUniqueUsersOutputPath = ""
      sparkConfFilePath = ""

      println("Parsing input parameters")
      println(args.length)
      println(args.mkString(" "))

      if (args.isEmpty || args.length < 4) {
        throw new MMInvalidParameterException("missing input arguments ")
      } else {

        eventsPath = args(0)
        impressionsPath = args(1)
        countOfEventsOutputPath = args(2)
        countOfUniqueUsersOutputPath = args(3)

        if (args.length >= 5) {
          sparkConfFilePath = args(4)
        }

        if (args.length >= 6) {
          debugMode = Try(args(5).toBoolean).getOrElse(false)
        }

        if (eventsPath.isEmpty) {
          print(s"Input arguments : ${args.mkString(" ")}")
          throw new MMInvalidParameterException("events.csv path is missing")
        }
        if (impressionsPath.isEmpty) {
          print(s"Input arguments : ${args.mkString(" ")}")
          throw new MMInvalidParameterException("impression.csv path is missing")
        }

        if (countOfEventsOutputPath.isEmpty) {
          print(s"Input arguments : ${args.mkString(" ")}")
          throw new MMInvalidParameterException("count_of_events.csv path is missing")
        }

        if (countOfUniqueUsersOutputPath.isEmpty) {
          print(s"Input arguments : ${args.mkString(" ")}")
          throw new MMInvalidParameterException("count_of_users.csv path is missing")
        }

        println(s"Reading events from, $eventsPath")
        println(s"Reading Impressions from, $impressionsPath")
        println(s"Writing count_of_events to, $countOfEventsOutputPath")
        println(s"Reading count_of_users to, $countOfUniqueUsersOutputPath")

        // initialise spark session
        logger.info("Starting Attribution App")
        val sparkSession = SparkSession.builder()
          //.config(getSparkAppConf)
          .getOrCreate()

        sparkSession.sparkContext.setLogLevel("ERROR")

        val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)

        println("----------------- Attribute analysis --------------------------")

        println("Reading events ....")
        val eventsDS = loadEventsDS(sparkSession, eventsPath, eventColNames)
        displayData(eventsDS, "Events count :")

        println("Deduplication of events within minute to avoid click on an ad twice by mistake.")

        val eventsAfterDedupDF = dedupEventsDS(sparkSession, eventsDS)
        displayData(eventsAfterDedupDF, "De-duplicated events count : ")

        println("Deduplication phase complete.")


        println("Reading impressions ....")
        val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)

        val impressionsDS = loadImpressionsDS(sparkSession, impressionsPath, impressionColNames)
        displayData(impressionsDS, "impressions count :")

        // - The count of attributed events for each advertiser, grouped by event type.
        val markAttributeEventsDF = fetchAttributeEvents(sparkSession, eventsAfterDedupDF, impressionsDS)
        displayData(markAttributeEventsDF, "Marked attribute events count : ")

        println("Caching attributed events")
        markAttributeEventsDF.persist()

        println("Generate stats :  The count of attributed events for each advertiser, grouped by event type")
        val attributedEventsByAdvertiserDF = calculateCountOfEvents(sparkSession, markAttributeEventsDF)

        if (debugMode) {
          attributedEventsByAdvertiserDF.printSchema()
          println(s" The count of attributed events dataset count : ${attributedEventsByAdvertiserDF.count()}")
        }
        attributedEventsByAdvertiserDF.show()

        println("Storing :  The count of attributed events for each advertiser, grouped by event type")
        storeDataToCsv(attributedEventsByAdvertiserDF, SaveMode.Overwrite, countOfEventsOutputPath)

        // - The count of unique users that have generated attributed events for each advertiser, grouped by event type.
        println("Generate stats :  The count of unique users that have generated attributed events for each advertiser, " +
          "grouped by event type.")
        val attributedUniqueUsersByAdvertiserDF = calculateCountOfUniqueEvents(sparkSession, markAttributeEventsDF)

        if (debugMode) {
          attributedUniqueUsersByAdvertiserDF.printSchema()
          println(s" The count of unique users that have generated attributed events " +
            s"for each advertiser count : ${attributedUniqueUsersByAdvertiserDF.count()}")
        }
        attributedUniqueUsersByAdvertiserDF.show()

        println("Storing :   The count of unique users that have generated attributed events for each advertiser, " +
          "grouped by event type.")
        storeDataToCsv(attributedUniqueUsersByAdvertiserDF, SaveMode.Overwrite, countOfUniqueUsersOutputPath)

        println("----------------- Attribute analysis completed!!! --------------------------")
        sparkSession.stop()
      }
    } catch {
      case invalidParameterException: MMInvalidParameterException =>
        usage(invalidParameterException.getMessage)
      case genEx: Exception =>
        println("Exception while running application.")
        genEx.printStackTrace()

    }
  }

  //  def getSparkAppConf: SparkConf = {
  //    val sparkAppConf = new SparkConf
  //    //Set all Spark Configs
  //    val props = new Properties
  //    props.load(Source.fromFile(sparkConf).bufferedReader())
  //    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
  //    sparkAppConf
  //  }
}

