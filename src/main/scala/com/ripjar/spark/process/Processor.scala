package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream



trait Processor {

  def process(stream: DStream[DataItem]): DStream[DataItem]
}

