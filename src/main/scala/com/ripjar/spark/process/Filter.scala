package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.Instance

/*
 * Used to filter the steam
 * 
 * Config parameters: 
 *  	None
 * 
 * Task parameters:
 * 		"filter": 
 *   		[
 *     			{"field": "xxx", "method": "out"}
 *   			{"field": "yyy", "method": "regex", "condition": "pattern"}
 *      	]	 	
 *
 */
//TODO: Complete
// Performs simple filters in the map based upon task parameters
class Filter(config: Instance) extends Processor with Serializable {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(filter(_))
  }

  def filter(item: DataItem): DataItem = {
 
    item
  }
}