package com.ripjar.spark.process

import com.ripjar.spark.job._
import com.ripjar.spark.data._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

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

  val inputPath = DataItem.toPathElements(config.getMandatoryParameter("input"))
  val duration = config.getMandatoryParameter("duration").toInt
  val slide_duration = config.getMandatoryParameter("slide_duration").toInt

  // defaulting on twitter.
  val splitOn = config.getParameter("split_on", " ")
  val matchOn = config.getParameter("match_on", "^#.*")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(input => {
      // make sure the Strings are not empty

      input.getTyped[String](inputPath) match {
        case None => ""
        case n => n.toString
      }
    }).flatMap(status => {
      // Generate words

      status.split(splitOn)
    }).filter(word => {
      // get those words starting with #

      word.matches(matchOn)
    }).map(tag => {
      // Convert words into tuples
      (tag, 1)
    }).reduceByKeyAndWindow(_ + _, _ - _, Seconds (duration), Seconds(slide_duration)
      ).map( (t: (String, Int)) => {
      // flip the tuple to have the count as the first term

      (t._2, t._1)
    }).transform(rdd => {
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
        val item = new DataItem()
        item.put(p._2, p._1)

        item
      })
    })
  }
}