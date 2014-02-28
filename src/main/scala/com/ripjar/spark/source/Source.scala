package com.ripjar.spark.source

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream

trait Source extends Serializable {

  def stream() : DStream[DataItem]
}