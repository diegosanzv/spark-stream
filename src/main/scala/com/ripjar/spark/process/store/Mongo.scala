package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import java.util.Properties
import kafka.producer._
import com.ripjar.spark.job.Instance

/*
 * Used to put a stream back onto kafka
 * 
 * Config parameters: 
 *  	brokers
 *   	route
 * 
 * Task parameters:
 * 		"route": "route" 
 * 
 * The tasks route overloads the default route
 */
//TODO: Test
object KafkaStorage {
  val logger = LoggerFactory.getLogger(classOf[KafkaStorage])

  var producer: Producer[String, Array[Byte]] = null
  var lock = ""

  def getProducer(broker: String): Producer[String, Array[Byte]] = {
    if (producer == null) {
      lock.synchronized {
        if (producer == null) {
          val prodProps = new Properties()
          prodProps.put("metadata.broker.list", broker)
          prodProps.put("serializer.class", "kafka.serializer.DefaultEncoder")

          val config = new ProducerConfig(prodProps)
          producer = new Producer[String, Array[Byte]](config)
        }
      }
    }
    producer
  }
}

class KafkaStorage(config: Instance) extends Processor with Serializable {

  val route: String = config.getMandatoryParameter("route")
  val brokers: String = config.getMandatoryParameter("brokers")
  val taskRoutePath = DataItem.toPathElements("task.kafka.route")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(store(_))
  }

  def store(input: DataItem): DataItem = {
    val data = input.toString.getBytes()

    val r = input.getTyped[String](taskRoutePath) match {
      case Some(p) => p
      case _ => route
    }

    val km = new KeyedMessage[String, Array[Byte]](r, data)
    val messages1 = Array[KeyedMessage[String, Array[Byte]]](km)
    KafkaStorage.getProducer(brokers).send(messages1: _*)

    input
  }

}