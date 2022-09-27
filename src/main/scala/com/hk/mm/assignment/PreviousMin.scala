package com.hk.mm.assignment

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class PreviousMin extends UserDefinedAggregateFunction {

  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("timestamp", IntegerType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("previousMin", IntegerType) :: Nil
  )

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

   override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println("Before : " + buffer(0))
    if (buffer.getAs[Int](0) == 0) {
      buffer(0) = input.getAs[Int](0);
    } else if ((input.getAs[Int](0) - buffer.getAs[Int](0)) > 60) {
      buffer(0) = input.getAs[Int](0);
    }
    println("After : " + buffer(0))
    println("--------------------------")
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println("merge Before : " + (buffer1.getAs[Int](0)));
    println("merge Before : " +(buffer2.getAs[Int](0)));
    if ((buffer2.getAs[Int](0) - buffer1.getAs[Int](0)) > 60) {
      buffer1(0) = buffer2.getAs[Int](0);
    }
    println(" merge After : " + buffer1(0))
    println("--------------------------")
  }

   override def evaluate(buffer: Row): Integer  = {
    println("-> evaluate({})", buffer.getInt(0))
    buffer.getInt(0)
  }
}