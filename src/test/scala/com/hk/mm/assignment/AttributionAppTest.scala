package com.hk.mm.assignment

import com.hk.mm.assignment.AttributionApp.{Event, Impression, loadEventsDS, loadImpressionsDS}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class AttributionAppTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("AttributionAppTest")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)
    val sampleDF = loadEventsDS(spark, "src/test/data/events.csv", eventColNames)
    val rCount = sampleDF.count()
    assert(rCount == 9, " record count should be 9")

    val impressionColNames = classOf[Impression].getDeclaredFields.map(ea => ea.getName)
    val impressionsDS = loadImpressionsDS(spark, "src/test/data/impressions.csv", impressionColNames)
    val impCount = impressionsDS.count()
    assert(impCount == 15, " record count should be 15")
  }

  //  test("Count by Country") {
  //    val sampleDF = loadSurveyDF(spark, "data/sample.csv")
  //    val countDF = countByCountry(sampleDF)
  //    val countryMap = new mutable.HashMap[String, Long]
  //    countDF.collect().foreach(r => countryMap.put(r.getString(0), r.getLong(1)))
  //
  //    assert(countryMap("United States") == 4, ":- Count for Unites States should be 6")
  //    assert(countryMap("Canada") == 2, ":- Count for Canada should be 2")
  //    assert(countryMap("United Kingdom") == 1, ":- Count for Unites Kingdom should be 1")
  //  }

}