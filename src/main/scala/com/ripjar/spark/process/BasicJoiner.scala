package com.ripjar.spark.process

import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.DataItem
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * A very simple Stream joiner that just produces one continuous
 * Stream of data items.
 *
 * Created by mike on 1/30/14.
 */
class BasicJoiner extends MultiProcessor {
  def flattenJoined(pair: (Int, (DataItem, Option[DataItem]))) : List[DataItem] = {
    val values = pair._2

    values._2 match {
      case Some(item) => List(values._1, item)
      case _          => List(values._1)
    }
  }

  def process(streams: Array[DStream[DataItem]]) : DStream[DataItem] = {
    if(streams.length <= 1) {
      return streams.head
    }

    val paired = streams.map(stream => {
      stream.map(item => {
        (item.hashCode, item)
      })
    }).toList

    // join them all up.
    paired.tail.foldLeft[DStream[(Int, DataItem)]](paired.head)( (joinStream, otherStream) => {
      joinStream.leftOuterJoin(otherStream).flatMap(flattenJoined)
    }).flatMap(flattenJoined)
  }
}
