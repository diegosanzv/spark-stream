package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig

/*
 * Creates recommendations from the stream
 * 
 * Config parameters: 

 * 
 * Task parameters:
 * 		 	
 *
 */
//TODO: Complete
class Recommender(config: InstanceConfig) extends Processor with Serializable {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(recommend(_))
  }

  def recommend(item: DataItem): DataItem = {
 
    item
  }
}