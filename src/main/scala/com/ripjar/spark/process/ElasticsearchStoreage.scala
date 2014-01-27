package com.ripjar.spark.process

import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.node.Node
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.ripjar.spark.job.Instance
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data._


/*
 * Stores the output in elasticsearch
 * 
 * Config parameters: 
 * 
 * Task parameters:
 * 		"store": {
 *   		"index": "client/project"
 *      }	 	
 *
 */
// TODO: Handle timeouts / failures
object ElasticsearchStoreage {
  val logger = LoggerFactory.getLogger(classOf[ElasticsearchStoreage])

  var client: Client = null

  def getClient(cluster: String): Client = {
    if (client == null) {
      client.synchronized {
        if (client == null) {

          val node = nodeBuilder().clusterName(cluster).client(true).node()
          client = node.client()
        }
      }
    }
    client
  }
}

class ElasticsearchStoreage(config: Instance) extends Processor with Serializable {

  val cluster: String = config.getMandatoryParameter("cluster")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(store(_))
  }

  def store(input: DataItem): DataItem = {
    val json: String = input.toString
//TODO - Complete    ElasticsearchStoreage.getClient(cluster).update(request)


    input
  }

}