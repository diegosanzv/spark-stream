package com.ripjar.spark.process

import com.ripjar.spark.job._
import com.ripjar.spark.data._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream


/*
 * Used to store the steam
 * 
 * Config parameters: 
 *  	hdfs file path
 * 
 * Task parameters:
 * 		"store": {
 *   		"location": "client/project"
 *      }	 	
 *
 */
// TODO: It is possible that task specifies further saving parameters. 
// If this is the case then will need to store an array of locations and pick the file based on that.
// Possibly having a filter matching the tasks
class FileStorage(config: Instance) extends Processor {
  val file = config.getMandatoryParameter("file")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.saveAsTextFiles(file)
    stream
  }
}