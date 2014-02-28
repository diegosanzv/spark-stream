package com.ripjar.spark.source

import com.ripjar.spark.job.SourceConfig
import com.ripjar.spark.data._
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import com.typesafe.config.ConfigFactory
import twitter4j.Status
import twitter4j.json.DataObjectFactory

/*

  configuration is required for the following keys

   consumer_key = "string"
   consumer_secret = "string"
   access_token = "string"
   access_token_secret = "string"

 */

class Twitter(config: SourceConfig, @transient ssc: StreamingContext) extends Source {

  val logger = LoggerFactory.getLogger("TwitterSource")
  val statusPath = new ItemPath("status")

  System.setProperty("twitter4j.oauth.consumerKey", config.getMandatoryParameter("consumer_key"))
  System.setProperty("twitter4j.oauth.consumerSecret", config.getMandatoryParameter("consumer_secret"))
  System.setProperty("twitter4j.oauth.accessToken", config.getMandatoryParameter("access_token"))
  System.setProperty("twitter4j.oauth.accessTokenSecret", config.getMandatoryParameter("access_token_secret"))

  def stream(): DStream[DataItem] = {
    TwitterUtils.createStream(ssc, None).map((status: Status) => {
      val json = DataObjectFactory.getRawJSON(status)

      DataItem.fromJSON(json)
    })
  }
}