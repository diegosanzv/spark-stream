package com.ripjar.spark.process

import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.DataItem
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.ripjar.spark.job.InstanceConfig

/**
 * A very simple Stream joiner that just produces one continuous
 * Stream of data items.
 *
 * Created by mike on 1/30/14.
 */
class BasicJoiner(config: InstanceConfig) extends MultiProcessor {
  def process(streams: Array[DStream[DataItem]]) : DStream[DataItem] = {
    if(streams.length <= 1) {
      return streams.head
    }

    val lst = streams.toList

    // join them all up.
    lst.tail.foldLeft[DStream[DataItem]](lst.head)( (joinStream, otherStream) => {
      joinStream.union(otherStream)
    })
  }
}
