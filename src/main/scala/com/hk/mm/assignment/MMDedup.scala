package com.hk.mm.assignment

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

case class Event(timestamp: Int, event_id: String, advertiser_id: Int, user_id: String, event_type: String)

case class PrevMinimumTimeStamp(var timestamp: Int)

object MMDedup extends Aggregator[Event, PrevMinimumTimeStamp, Int] {

  def zero: PrevMinimumTimeStamp = PrevMinimumTimeStamp(0)

  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: PrevMinimumTimeStamp,
             eventt: Event): PrevMinimumTimeStamp = {
    if (buffer.timestamp - eventt.timestamp > 60) {
      buffer.timestamp = eventt.timestamp;
    }
    buffer
  }

  // Merge two intermediate values
  def merge(b1: PrevMinimumTimeStamp,
            b2: PrevMinimumTimeStamp): PrevMinimumTimeStamp = {
    if (b1.timestamp - b2.timestamp > 60) {
      b1.timestamp = b2.timestamp;
    }
    b1
  }

  // Transform the output of the reduction
  def finish(reduction: PrevMinimumTimeStamp): Int = reduction.timestamp

  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[PrevMinimumTimeStamp] = Encoders.product

  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Int] = Encoders.scalaInt
}