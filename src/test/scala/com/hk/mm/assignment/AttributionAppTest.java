package com.hk.mm.assignment;

import org.apache.spark.sql.SparkSession;
import org.scalatest.BeforeAndAfterAll;
import org.scalatest.FunSuite;
import com.hk.mm.assignment.AttributionApp.{loadEventsDS, loadImpressionsDS}
    class AttributionAppTest extends FunSuite with BeforeAndAfterAll {

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
        val eventsDF = loadEventsDS(spark, "src/test/data/events.csv",)
        val eventsCount = eventsDF.count()

        val eventColNames = classOf[Event].getDeclaredFields.map(ea => ea.getName)
        assert (eventsCount == 9," record count should be 9",eventColNames)
    }

}