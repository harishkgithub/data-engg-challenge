package com.hk.mm.assignment

import com.hk.mm.assignment.AttributionApp.{Event, Impression, calculateCountOfEvents, dedupEventsDS, fetchAttributeEvents, loadEventsDS, loadImpressionsDS}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class AttributionAppTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var eventsDS: Dataset[Event] = _
  @transient var impressionsDS: Dataset[Impression] = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AttributionAppTest")
      .master("local[3]")
      .getOrCreate()

    val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)
    eventsDS = loadEventsDS(spark, "src/test/data/events.csv", eventColNames)

    val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)
    impressionsDS = loadImpressionsDS(spark, "src/test/data/impressions.csv", impressionColNames)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    assert(eventsDS.count() == 9, " record count should be 9")
    assert(impressionsDS.count() == 15, " record count should be 15")
  }

  test("Count of attribute Events") {

    val eventsAfterDedupDF = dedupEventsDS(spark, eventsDS)
    // - The count of attributed events for each advertiser, grouped by event type.
    val markAttributeEventsDF = fetchAttributeEvents(spark, eventsAfterDedupDF, impressionsDS)

    markAttributeEventsDF.persist()

    val attributedEventsByAdvertiserDF = calculateCountOfEvents(spark, markAttributeEventsDF)
    assert(attributedEventsByAdvertiserDF.count() == 2, "attribute events count should be 2")

  }

}