package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig


/*
 * Logs the stream
 */
object Log {
  val logger = LoggerFactory.getLogger(classOf[Log])
}

class Log(config: InstanceConfig) extends Processor with Serializable {

  override def process(input: DStream[DataItem]): DStream[DataItem] = {
    input.map(print(_))
  }

  def print(input: DataItem): DataItem = {
    val json = input.toString
    Log.logger.info(json)
    input
  }

}