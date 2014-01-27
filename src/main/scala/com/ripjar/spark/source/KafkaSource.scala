package com.ripjar.spark.source

import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.DataItem
import com.ripjar.spark.job.SourceCfg
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel
import kafka.serializer.{ DefaultDecoder, StringDecoder }
import com.ripjar.tophat.dto.ser.ProcessRequestSerializer
import scala.collection.JavaConversions._
import scala.Array.canBuildFrom

/**
 * Created by mike on 1/22/14.
 */

object KafkaSource {

  def parseBinaryInput(raw: Array[Byte]): DataItem = {
    val pr = ProcessRequestSerializer.inflate(raw, true)
    val json = pr.getMetadata().toString()
    val dataitem = DataItem.fromJson(json)
    if (pr.getData() != null)
      dataitem.raw = pr.getData.array()
    dataitem
  }

  def parseTextInput(json: String): DataItem = {
    DataItem.fromJson(json)
  }

}

class KafkaSource(config: SourceCfg, ssc: StreamingContext) extends Source {

  val group = config.getMandatoryParameter("group")
  val zkQuorum = config.getMandatoryParameter("zkQuorum")
  val topics = config.getMandatoryParameter("topics")
  val numThreads: Int = config.getMandatoryParameter("numThreads").toInt

  val kafkaParams = Map[String, String](
    "zookeeper.connect" -> zkQuorum,
    "group.id" -> group,
    "zookeeper.connection.timeout.ms" -> "10000")

  val topicpMap = topics.split(",").map((_, numThreads)).toMap

  def stream(): DStream[DataItem] = {
    val isBinaryStream: Boolean = config.getMandatoryParameter("binary").toBoolean

    if (isBinaryStream) {
      val stream: DStream[(String, Array[Byte])] = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicpMap, StorageLevel.MEMORY_ONLY_SER_2)
      stream.map(input => {
        KafkaSource.parseBinaryInput(input._2)
      })
    } else {
      val stream: DStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicpMap, StorageLevel.MEMORY_ONLY_SER_2)
      stream.map(input => {
        KafkaSource.parseTextInput(input._2)
      })
    }

  }
}
