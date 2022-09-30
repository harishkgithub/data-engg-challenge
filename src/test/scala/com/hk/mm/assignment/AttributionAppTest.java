package com.hk.mm.assignment;

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

class AttributionAppTest /*extends FunSuite with BeforeAndAfterAll*/ {

    @transient var spark:SparkSession = _

    override def beforeAll():Unit ={
        spark = SparkSession.builder()
                .appName("HelloSparkTest")
                .master("local[1]")
                .getOrCreate()
    }

    override def afterAll(): Unit = {
        spark.stop()
    }

    test("Data File Loading") {
        val eventsDF = loadSurveyDF(spark, "src/test/data/events.csv")
        val eventsCount = eventsDF.count()
        assert (eventsCount == 9," record count should be 9")
    }

}