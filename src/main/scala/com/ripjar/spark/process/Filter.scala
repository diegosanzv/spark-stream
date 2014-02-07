package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig
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
case class FilterConfig(val items: List[FilterConfigItem])
case class FilterConfigItem(val field: String, val method: String, val condition: String = "")

object Filter {
  val path: List[String] = DataItem.toPathElements("tasks.filter")
}

class Filter(config: InstanceConfig) extends Processor with Serializable {

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

  val filters: FilterConfig = config.parameters.get("filter") match {
    case Some(json) => parseJson(json)
    case _ => new FilterConfig(List[FilterConfigItem]())
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    filters.items.foldLeft(stream)((stream: DStream[DataItem], filtr) => {
      filtr.method match {

          // regexes filter based on a match
          case "regex" => {
            stream.filter(item => {
              item.get[String](new ItemPath(filtr.field)) match {
                case Some(x) => !x.matches(filtr.condition)
                case _ => true
              }
            })
          }

          // outs remove the fields from a data item. It's really a mapping
          case "out" => {
            stream.map(item => {
              item.remove(new ItemPath(filtr.field))

              item
            })
          }

          case _ => stream
        }
    })
  }

 /* def filter(item: DataItem): Boolean = {
    def test(fci: FilterConfigItem): Boolean = {
      //TODO: Complete
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
      case _ => filters
    }
    fliterConfig.items.forall(fci => { test(fci) })
  }*/
}