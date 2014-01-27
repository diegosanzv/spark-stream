package com.ripjar.spark.process

import com.ripjar.spark.data._
import com.ripjar.zmq.ZMQWorker
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Boolean
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.Instance

object ExternalZeroMQ {
  val logger = LoggerFactory.getLogger(classOf[ExternalZeroMQ])
}

/*
 * Used to pass jobs to out of process methods such as ner
 * 
 * Config parameters: 
 *  	connection string
 * 
 * Task parameters:
 * 		Just pass on the task block	 	
 *
 */
// TODO: Handle timeouts / failures
class ExternalZeroMQ(config: Instance) extends Processor with Serializable {
  val connectStr = config.getMandatoryParameter("connectStr")

  override def process(input: DStream[DataItem]): DStream[DataItem] = {
    input.map(process(_))
  }

  def process(input: DataItem): DataItem = {
    val zmqWorker: ZMQWorker = new ZMQWorker
    val json = input.toString
    val rtn = zmqWorker.doWork(connectStr, json)
    DataItem.fromJson(rtn)
  }
}