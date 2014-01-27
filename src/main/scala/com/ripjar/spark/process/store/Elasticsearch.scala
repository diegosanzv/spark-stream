package com.ripjar.spark.process.store

import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.node.Node
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.ripjar.spark.job.Instance
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data._
import com.ripjar.spark.process.Processor
import org.elasticsearch.action.index.IndexRequestBuilder

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
// TODO: Test
object Elasticsearch {
  val logger = LoggerFactory.getLogger(classOf[Elasticsearch])

  var indexes: Map[String, IndexRequestBuilder] = Map.empty

  def getIndex(elasticConfig: ElasticsearchConfig): IndexRequestBuilder = {
    indexes.get(elasticConfig.hash) match {
      case Some(c) => c
      case None => {
        indexes.synchronized {
          indexes.get(elasticConfig.hash) match {
            case Some(c) => c
            case None => {
              val node = nodeBuilder().clusterName(elasticConfig.cluster).client(true).node()
              val client = node.client()
              val index = client.prepareIndex(elasticConfig.index, elasticConfig.doctype)
              indexes += ((elasticConfig.hash, index))
              index
            }
          }
        }
      }
    }
  }

}

class ElasticsearchConfig(val cluster: String, val index: String, val doctype: String) {
  val hash = cluster + ":" + index + ":" + doctype
}

class Elasticsearch(config: Instance) extends Processor with Serializable {

  val elasticConfig = new ElasticsearchConfig(
    config.getMandatoryParameter("cluster"),
    config.getMandatoryParameter("index"),
    config.getMandatoryParameter("doctype"))

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(store(_))
  }

  def store(input: DataItem): DataItem = {
    val json: String = input.toString

    Elasticsearch.getIndex(elasticConfig).setSource(json).execute()
    input
  }

}