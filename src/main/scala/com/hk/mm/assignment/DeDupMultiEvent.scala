package com.hk.mm.assignment

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class DeDupMultiEvent extends UserDefinedAggregateFunction {
  val AGG_TIME_TO_DISCARD_IN_SEC = 60

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
    val toBeUpdateBy = input.getAs[Int](0)
    val bufferValue = buffer.getAs[Int](0)

    if (bufferValue == 0) {
      buffer(0) = toBeUpdateBy
    } else if ((toBeUpdateBy - bufferValue) > AGG_TIME_TO_DISCARD_IN_SEC) {
      buffer(0) = toBeUpdateBy
    }

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val buffer2Value = buffer2.getAs[Int](0)

    if ((buffer2Value - buffer1.getAs[Int](0)) > AGG_TIME_TO_DISCARD_IN_SEC) {
      buffer1(0) = buffer2Value
    }

  }

  override def evaluate(buffer: Row): Integer = {
    buffer.getInt(0)
  }
}