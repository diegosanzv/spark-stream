package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig

/*
 * Performs simple movements and merges in the map based upon task parameters
 * Maybe should be merged with filter
 * 
 * Config parameters: 
 *  	None
 * 
 * Task parameters:
 * 		"transform": 
 *   		[
 *     			{"move": "xxx", "to": "yyy"},
 *   			{"copy": "xxx", "to": "yyy"}
 *      	]	 	
 *
 */
//TODO: Complete
class Transform(config: InstanceConfig) extends Processor with Serializable {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(transform(_))
  }

  def transform(item: DataItem): DataItem = {
    
    
    item
  }
}