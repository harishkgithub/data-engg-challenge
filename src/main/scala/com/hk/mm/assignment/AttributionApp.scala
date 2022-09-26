package com.hk.mm.assignment

object AttributionApp {
  def main(args: Array[String]): Unit = {

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
    case class Event(timestamp: Int, event_id: String,advertiser_id: Int,user_id: String,event_type: String)


//      ### Impressions
//        This dataset contains a series of ads displayed to users online for different advertisers:impressions.csv
//      ** Schema :**
//      Column number | Column name | Type | Description
//      ------------- | ------------- | ------------- | -------------
//      1 | timestamp | integer | Unix timestamp when the impression was served.
//      2 | advertiser_id | integer | The advertiser ID that owns the ad that was displayed.
//      3 | creative_id | integer | The creative (or ad) ID that was displayed.
//      4 | user_id | string(UUIDv4) | An anonymous user ID this ad was displayed to.
    case class Impression(timestamp: Int, event_id: String,advertiser_id: Int,user_id: String,event_type: String)

    import scala.reflect.ClassTag
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
      org.apache.spark.sql.Encoders.kryo[A](ct)

    // initialise spark session (running in "local" mode)
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Hello World")
       .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    // do stuff
    println("Hello, world!")
    val eventsDFCsv = sparkSession.read.csv("src/resources/events.csv").as[Event]

    eventsDFCsv.printSchema()
    eventsDFCsv.show(100,true);

    val impressionsDFCsv = sparkSession.read.csv("src/resources/impressions.csv").as[Impression]

    impressionsDFCsv.printSchema()
    impressionsDFCsv.show(100, true);

    print("impressionsDFCsv count " +impressionsDFCsv.count());

    // terminate underlying spark context
    sparkSession.stop()
  }
}
