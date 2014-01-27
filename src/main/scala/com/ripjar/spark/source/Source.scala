package com.ripjar.spark.source

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream

trait Source {

  def stream() : DStream[DataItem]
}