package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig

/*
 * Creates summaizations from the stream
 * 
 * Config parameters: 

 * 
 * Task parameters:
 * 		 	
 *
 */
//TODO: Complete
class Summariser(config: InstanceConfig) extends Processor with Serializable {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(summarize(_))
  }

  def summarize(item: DataItem): DataItem = {
 
    item
  }
}