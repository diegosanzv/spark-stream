package com.ripjar.spark.process.store

import com.ripjar.spark.job._
import com.ripjar.spark.data._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.process.{TerminalProcessor, Processor}


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
class File(config: InstanceConfig) extends TerminalProcessor {
  val file = config.getMandatoryParameter("file")
  val asObject = config.getParameter("asObject", "false").toBoolean

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    if(asObject) {
      stream.saveAsObjectFiles(file)
    } else {
      stream.saveAsTextFiles(file)
    }

    stream
  }
}