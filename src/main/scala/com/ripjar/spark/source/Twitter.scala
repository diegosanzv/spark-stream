package com.ripjar.spark.source

import com.ripjar.spark.job.SourceCfg
import com.ripjar.spark.data._
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import com.typesafe.config.ConfigFactory

/*

  configuration is required for the following keys

   consumer_key = "string"
   consumer_secret = "string"
   access_token = "string"
   access_token_secret = "string"

 */

class Twitter(val config: SourceCfg, val ssc: StreamingContext) extends Source {

  val logger = LoggerFactory.getLogger("TwitterSource")

  System.setProperty("twitter4j.oauth.consumerKey", config.getMandatoryParameter("consumer_key"))
  System.setProperty("twitter4j.oauth.consumerSecret", config.getMandatoryParameter("consumer_secret"))
  System.setProperty("twitter4j.oauth.accessToken", config.getMandatoryParameter("access_token"))
  System.setProperty("twitter4j.oauth.accessTokenSecret", config.getMandatoryParameter("access_token_secret"))

  def stream(): DStream[DataItem] = {
    println("Twitter stream requested")

    TwitterUtils.createStream(ssc, None).map(status => {
      val item = new DataItem()

      item.put("status", status.getText())

      item
    })
  }
}