package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.ripjar.product.orac.analytic.FeatureExtraction
import com.ripjar.product.orac.analytic.process.FeatureProcessor
import scala.collection.JavaConversions
import com.ripjar.spark.job.InstanceConfig
import java.util.{ Map => JMap }
import java.util.{ HashMap => JHashMap }
import java.lang.{ String => JString }
import java.lang.{ Object => JObject }

/*
 * Performs feature enrichment on the stream
 * 
 * Config parameters: 
 *  	input
 *   	resources path 
 *  
 *  input - default location where to read the text from
 *     	
 * 
 * Task parameters:
 * 		"enrich": {
 *   		"field": "twitter.message"
 *   	} 
 * 
 * The tasks route overloads the default route
 */
//TODO: Set resources
object LegacyFeatureEnrich {
  val logger = LoggerFactory.getLogger(classOf[LegacyFeatureEnrich])

  var featureProcessor: FeatureProcessor = null

  def getFeatureProcessor(resources: String): FeatureProcessor = {
    if (featureProcessor == null) {
      featureProcessor.synchronized {
        if (featureProcessor == null) {
          System.setProperty("resources", resources)
          featureProcessor = new FeatureProcessor
        }
      }
    }
    featureProcessor
  }

}

// Performs enrichment using methods form MM phase 1
class LegacyFeatureEnrich(config: InstanceConfig) extends Processor with Serializable {

  val resources = config.getMandatoryParameter("resources")
  val defaultTextPath = new ItemPath(config.getMandatoryParameter("input"))
  val taskTextPath = new ItemPath("task.enrich.field")

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(enrich(_))
  }

  def enrich(item: DataItem): DataItem = {
    val textOption: Option[String] = item.get[String](taskTextPath) match {
      case Some(path) => {
        item.get[String](new ItemPath(path))
      }
      case _ => item.get[String](defaultTextPath)
    }

    textOption match {
      case Some(value: String) => {
        val lm: JMap[JString, JObject] = new JHashMap[JString, JObject]()
        lm.put("message", value)
        LegacyFeatureEnrich.getFeatureProcessor(resources).process(lm)
        lm.remove("message")
        DataItem.merge(item, lm)
      }
      case _ => //Do nothing
    }

    item
  }
}

