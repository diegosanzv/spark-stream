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

  def pairedMap(item: DataItem) : (Int, DataItem) = {
    (item.hashCode, item)
  }

  def process(streams: Array[DStream[DataItem]]) : DStream[DataItem] = {
    if(streams.length <= 1) {
      return streams.head
    }

    val lst = streams.toList

    // join them all up.
    lst.tail.foldLeft[DStream[DataItem]](lst.head)( (joinStream, otherStream) => {
      val otherMapped = otherStream.map(pairedMap)

      joinStream.map(pairedMap).leftOuterJoin(otherMapped).flatMap(flattenJoined)
    })
  }
}
