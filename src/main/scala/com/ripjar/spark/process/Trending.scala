package com.ripjar.spark.process

import com.ripjar.spark.job._
import com.ripjar.spark.data._
import org.apache.spark.streaming._
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

  // defaulting on twitter.
  val splitOn = config.getParameter("split_on", " ")
  val matchOn = config.getParameter("match_on", "^#.*")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = sliding_window(stream)

  def sliding_window(stream: DStream[DataItem]) : DStream[DataItem] = {
    val count_path = new ItemPath("count")
    val generation_path = new ItemPath("generation")

    stream.filter( item => {
      // confirm we have the tags
      item.contains(inputPath)
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
      pair._2
    }).transform( (rdd: RDD[DataItem], time: Time) => {
      rdd.map( di => {
        di.put(generation_path, time.milliseconds)

        di
      })
    })
  }

  def slide_pair(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(input => {
      // make sure the Strings are not empty

      input.get[String](inputPath) match {
        case Some(n) => n.toString
        case _ => ""
      }
    }).flatMap(status => {
      // Generate words
      // TODO: Looks like twitter already gives us the tags at dataset.tweet.entities.hashtags array

      status.split(splitOn)
    }).filter(word => {
      // get those words starting with #

      word.startsWith("#")
    }).map(tag => {
      // Convert words into tuples
      (tag, 1)
    }).reduceByKeyAndWindow(_ + _, _ - _, Seconds (duration), Seconds(slide_duration)
      ).map( (t: (String, Int)) => {
      // flip the tuple to have the count as the first term

      (t._2, t._1)
    }).transform(rdd => {
      val generation = System.currentTimeMillis()
      var average = 0.0

      if(rdd.count() > 0) {
        // compute average
        average = rdd.map( (p:(Int, String)) => {
          p._1
        }).reduce(_ + _).toDouble / rdd.count().toDouble
      }

      // filter out only those above average
      rdd.filter( (p:(Int, String)) => {
        p._1 >= average
      }).map( (p: (Int, String)) => {
        val item = DataItem.create()

        item.put(new ItemPath("generation"), generation)
        item.put(new ItemPath("count"), p._1)
        item.put(new ItemPath("tag"), p._2)

        item
      })
    })
  }
}