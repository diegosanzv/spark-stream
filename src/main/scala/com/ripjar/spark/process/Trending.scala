package com.ripjar.spark.process

import com.ripjar.spark.job._
import com.ripjar.spark.data._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

/*
 * Performs trending on the stream
 * 
 * 
 * Config parameters: 
 *  	input - default field to read from
 *   	duration - default emit frequency
 *    	slide_duration - default 
 *      split_on - what to tokenize the field on - if not present whole field
 *      match - regex to match on a token
 * 
 * Task parameters:
 * 		"trend": 
 *   		[{
 *  			input - default field to read from
 *   			duration - default emit frequency
 *    			slide_duration - default 
 *      		split_on - what to tokenize the field on - if not present whole field
 *      		match - regex to match on a token
 *      	}, ...]	 	
 *
 */
class Trending(config: InstanceConfig) extends Processor with Serializable {

  val inputPath = new ItemPath(config.getParameter("input", "tag"))
  val duration = config.getMandatoryParameter("duration").toInt
  val slide_duration = config.getMandatoryParameter("slide_duration").toInt
  val min_count = config.getParameter("min_count", "2").toInt
  val topn = config.getParameter("topn", "200").toInt

  override def process(stream: DStream[DataItem]): DStream[DataItem] = sliding_window(stream)

  def sliding_window(stream: DStream[DataItem]) : DStream[DataItem] = {
    val count_path = new ItemPath("count")
    val generation_path = new ItemPath("generation")

    stream.filter( item => {
      // confirm we have the tags
      item.contains[Any](inputPath)
    }).map( item => {
      // Make the data items
      item.put(count_path, 1)

      (item.getMandatory[Any](inputPath), item)
    }).reduceByKeyAndWindow( (di1: DataItem, di2: DataItem) => {
      // forward reduction
      di1.put(count_path, di1.getMandatory[Integer](count_path) + di2.getMandatory[Integer](count_path))

      di1
    }, (di1: DataItem, di2: DataItem) => {
      // inverse reduction
      di1.put(count_path, di1.getMandatory[Integer](count_path) - di2.getMandatory[Integer](count_path))

      di1
    }, Seconds (duration),
       Seconds(slide_duration)).filter( (pair: (Any, DataItem)) => {
      pair._2.getMandatory[Integer](count_path) >= min_count
    }).map( (pair: (Any, DataItem)) => {
      (pair._2.getMandatory[Integer](count_path), pair._2)
    }).transform( (rdd: RDD[(Integer, DataItem)], time: Time) => {
      val top = rdd.sortByKey(ascending = false).map( pair => {
        pair._2.put(generation_path, time.milliseconds)

        pair._2
      }).take(topn)

      rdd.context.parallelize(top, 1)
    })
  }
}