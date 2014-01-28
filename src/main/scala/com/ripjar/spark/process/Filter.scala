package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.Instance
import org.json4s._
import org.json4s.native.JsonMethods._
import com.ripjar.spark.job.SparkJobException
import com.ripjar.spark.job.SparkJobErrorType

/*
 * Used to filter the steam
 * 
 * Config parameters: 
 *  	filter - a JSON structured set of filters - same as the task ones
 * 
 * Task parameters:
 * 		"filter": 
 *   		[
 *     			{"field": "xxx", "method": "out"}
 *   			{"field": "yyy", "method": "regex", "condition": "pattern"}
 *      	]	 	
 *
 */
//TODO: Complete
// Performs simple filters in the map based upon task parameters

case class FilterConfig(val items: List[FilterConfigItem])
case class FilterConfigItem(val field: String, val method: String, val condition: String = "")

object Filter {
  val path: List[String] = DataItem.toPathElements("tasks.filter")
}

class Filter(config: Instance) extends Processor with Serializable {

  private def parseJson(json: String): FilterConfig = {
    val jvRoot: JValue = parse(json)
    jvRoot match {
      case joRoot: JObject => {
        implicit val formats = DefaultFormats
        joRoot.extract[FilterConfig]
      }
      case _ => {
        throw new SparkJobException("Cannot parse the JSON config file containing the filter config: ".format(json), SparkJobErrorType.InvalidConfig)

      }
    }
  }

  val defaultFlilterConfig: FilterConfig = config.parameters.get("filter") match {
    case Some(json) => parseJson(json)
    case _ => new FilterConfig(List[FilterConfigItem]())
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.filter(filter(_))
  }

  def filter(item: DataItem): Boolean = {
    def test(fci: FilterConfigItem): Boolean = {
      //TODO
      true
    }

    val fliterConfig: FilterConfig = item.getTyped[DataItemList](Filter.path) match {
      case Some(dil: DataItemList) => {
        val additionalFilters: List[FilterConfigItem] = dil.value.map(di => {
          new FilterConfigItem("", "", "")
        })
        val filters: List[FilterConfigItem] = defaultFlilterConfig.items ++ additionalFilters
        new FilterConfig(filters)
      }
      case _ => defaultFlilterConfig
    }
    fliterConfig.items.forall(fci => { test(fci) })
  }
}