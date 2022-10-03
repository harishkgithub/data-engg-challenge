package com.hk.mm.assignment

import com.hk.mm.assignment.AttributionApp.{Event, Impression, calculateCountOfEvents, calculateCountOfUniqueEvents, dedupEventsDS, fetchAttributeEvents, loadEventsDS, loadImpressionsDS}
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

    attributedEventsByAdvertiserDF.show()
    attributedEventsByAdvertiserDF.printSchema()
    val attributeEventByAdvertiserMap = new mutable.HashMap[Int, Long]
    attributedEventsByAdvertiserDF.collect().foreach(r =>
      attributeEventByAdvertiserMap.put(r.getInt(0), r.getLong(2)))

    assert(attributeEventByAdvertiserMap(2) == 1, ":- Count events for advertiser_id 2 is 1")
    assert(attributeEventByAdvertiserMap(1) == 1, ":- Count events for advertiser_id 1 is 1")

    attributedUniqueUsersByAdvertiserDF.show()
    attributedUniqueUsersByAdvertiserDF.printSchema()
    val attributedUniqueUsersByAdvertiserMap = new mutable.HashMap[Int, Long]
    attributedEventsByAdvertiserDF.collect().foreach(ea =>
      attributedUniqueUsersByAdvertiserMap.put(ea.getInt(0), ea.getLong(2)))

    assert(attributeEventByAdvertiserMap(2) == 1, ":- Count unique users for advertiser_id 2 is 1")
    assert(attributeEventByAdvertiserMap(1) == 1, ":- Count unique users  for advertiser_id 1 is 1")

  }

}