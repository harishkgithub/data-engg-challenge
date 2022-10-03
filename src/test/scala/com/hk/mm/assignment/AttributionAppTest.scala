package com.hk.mm.assignment

import com.hk.mm.assignment.AttributionApp.{Event, Impression, calculateCountOfEvents, calculateCountOfUniqueEvents, dedupEventsDS, fetchAttributeEvents, loadEventsDS, loadImpressionsDS}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class AttributionAppTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var eventsDS: Dataset[Event] = _
  @transient var impressionsDS: Dataset[Impression] = _

  @transient var events_1DS: Dataset[Event] = _
  @transient var impressions_1DS: Dataset[Impression] = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AttributionAppTest")
      .master("local[3]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)
    eventsDS = loadEventsDS(spark, "src/test/data/events.csv", eventColNames)
    events_1DS = loadEventsDS(spark, "src/test/data/testevents.csv", eventColNames)

    val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)
    impressionsDS = loadImpressionsDS(spark, "src/test/data/impressions.csv", impressionColNames)
    impressions_1DS = loadImpressionsDS(spark, "src/test/data/testimpressions.csv", impressionColNames)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    assert(eventsDS.count() == 9, " record count should be 9")
    assert(impressionsDS.count() == 15, " record count should be 15")
  }

  test("Count of attribute Events and unique users") {

    val eventsAfterDedupDF = dedupEventsDS(spark, eventsDS)
    // - The count of attributed events for each advertiser, grouped by event type.
    val markAttributeEventsDF = fetchAttributeEvents(spark, eventsAfterDedupDF, impressionsDS)

    markAttributeEventsDF.persist()

    val attributedEventsByAdvertiserDF = calculateCountOfEvents(spark, markAttributeEventsDF)
    assert(attributedEventsByAdvertiserDF.count() == 2, "attribute events count should be 2")

    val attributedUniqueUsersByAdvertiserDF = calculateCountOfUniqueEvents(spark, markAttributeEventsDF)
    assert(attributedUniqueUsersByAdvertiserDF.count() == 2, "attribute unique users count should be 2")

    attributedEventsByAdvertiserDF.printSchema()
    val attributeEventByAdvertiserMap = new mutable.HashMap[Int, Long]
    attributedEventsByAdvertiserDF.collect().foreach(r =>
      attributeEventByAdvertiserMap.put(r.getInt(0), r.getLong(2)))

    assert(attributeEventByAdvertiserMap(2) == 1, ":- Count events for advertiser_id 2 is 1")
    assert(attributeEventByAdvertiserMap(1) == 1, ":- Count events for advertiser_id 1 is 1")

    attributedUniqueUsersByAdvertiserDF.printSchema()
    val attributedUniqueUsersByAdvertiserMap = new mutable.HashMap[Int, Long]
    attributedEventsByAdvertiserDF.collect().foreach(ea =>
      attributedUniqueUsersByAdvertiserMap.put(ea.getInt(0), ea.getLong(2)))

    assert(attributeEventByAdvertiserMap(2) == 1, ":- Count unique users for advertiser_id 2 is 1")
    assert(attributeEventByAdvertiserMap(1) == 1, ":- Count unique users  for advertiser_id 1 is 1")

  }

  test("Test dedup events") {
    //Count of raw events
    assert(events_1DS
      .filter(col("user_id") === "60000000-fd7e-48e4-aa61-3c14c9c714d5")
      .filter(col("event_type") === "click")
      .filter(col("advertiser_id") === 1)
      .filter(col("timestamp") > 1450631449)
      .filter(col("timestamp") < 1450631509)
      .count() == 4
      , "Count events for 60000000-fd7e-48e4-aa61-3c14c9c714d5 with type click is 4")

    val eventsAfterDedupDF = dedupEventsDS(spark, events_1DS)
    //Checking if 3 events are correctly discarded as they are from
    // same user for same advertiser with time difference of 60 seconds
    assert(eventsAfterDedupDF
      .filter(col("user_id") === "60000000-fd7e-48e4-aa61-3c14c9c714d5")
      .filter(col("event_type") === "click")
      .filter(col("advertiser_id") === 1)
      .filter(col("timestamp") > 1450631449)
      .filter(col("timestamp") < 1450631509)
      .count() == 1, "Count events for 60000000-fd7e-48e4-aa61-3c14c9c714d5 with type click and advertiser_id 1 is 1")

    assert(eventsAfterDedupDF
      .filter(col("user_id") === "60000000-fd7e-48e4-aa61-3c14c9c714d5")
      .filter(col("event_type") === "purchase")
      .filter(col("advertiser_id") === 1)
      .filter(col("timestamp") > 1450631449)
      .filter(col("timestamp") < 1450631509)
      .count() == 1, "Even though there is an  events for 60000000-fd7e-48e4-aa61-3c14c9c714d5 with type purchase and advertiser_id 1 withing " +
      "1 minute time range its not filtered as the event type is different purchase and same as previous click")

    /* Attribution test case*/
    assert(eventsAfterDedupDF
      .filter(col("advertiser_id") === 11)
      .filter(col("user_id") === "harish")
      .filter(col("event_type") === "click")
      .filter(col("timestamp") < 1450631930).count() == 1, "There is 1 event of " +
      "user harish with event type click from advertiser_id 11")
    val markAttributeEventsDF = fetchAttributeEvents(spark, eventsAfterDedupDF, impressions_1DS)

    //Events before impression should not be marked as attributed event
    assert(markAttributeEventsDF
      .filter(col("advertiser_id") === 11)
      .filter(col("user_id") === "harish")
      .filter(col("event_type") === "click")
      .filter(col("timestamp") < 1450631930).count() == 0,
      "there is an impression at 1450631930 so there should not be any event before 1450631930 of " +
        "user harish with event type click from advertiser_id 11")

    //Events after impression should be marked as attributed event
    assert(markAttributeEventsDF
      .filter(col("advertiser_id") === 11)
      .filter(col("user_id") === "harish")
      .filter(col("event_type") === "click")
      .filter(col("timestamp") > 1450631930)
      .filter(col("timestamp") < 1450632049).count() == 1,
      "there is an impression at 1450631930 so any event after 1450631930 with " +
        "user harish with event type click from advertiser_id 11 should be marked as attributed event")

  }

}