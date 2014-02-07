package com.ripjar.spark.process.store

import com.ripjar.spark.data._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import java.util.Properties
import kafka.producer._
import com.ripjar.spark.job.InstanceConfig
import com.ripjar.spark.process.Processor

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
object Kafka {
  val logger = LoggerFactory.getLogger(classOf[Kafka])

  var produces: Map[String, Producer[String, Array[Byte]]] = Map.empty

  def getProducer(broker: String): Producer[String, Array[Byte]] = {
    produces.get(broker) match {
      case Some(c) => c
      case None => {
        produces.synchronized {
          produces.get(broker) match {
            case Some(c) => c
            case None => {
              val prodProps = new Properties()
              prodProps.put("metadata.broker.list", broker)
              prodProps.put("serializer.class", "kafka.serializer.DefaultEncoder")

              val config = new ProducerConfig(prodProps)
              val producer = new Producer[String, Array[Byte]](config)
              produces += ((broker, producer))
              producer
            }
          }
        }
      }
    }
  }
}

class Kafka(config: InstanceConfig) extends Processor with Serializable {

  val route: String = config.getMandatoryParameter("route")
  val brokers: String = config.getMandatoryParameter("brokers")
  val taskRoutePath = new ItemPath("task.kafka.route")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(store(_))
  }

  def store(input: DataItem): DataItem = {
    val data = input.toString.getBytes()

    val r = input.get[String](taskRoutePath) match {
      case Some(p) => p
      case _ => route
    }

    val km = new KeyedMessage[String, Array[Byte]](r, data)
    val messages1 = Array[KeyedMessage[String, Array[Byte]]](km)
    Kafka.getProducer(brokers).send(messages1: _*)

    input
  }

}