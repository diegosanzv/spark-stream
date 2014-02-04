package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.InstanceConfig
import org.apache.spark.streaming.StreamingContext._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

/*
 * Creates summaizations from the stream
 * 
 * Config parameters: 

 * 
 * Task parameters:
 * 		 	
 *
 */
class Summariser(config: InstanceConfig) extends Processor with Serializable {
  val key = DataItem.toPathElements(config.getMandatoryParameter("unique_key"))
  val summations = parseData(config.data)

  private def parseData(data: JValue): Array[SumItem] = {
    implicit val formats = DefaultFormats

    data.extract[Array[SumItem]]
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(item => {
      (item.getMandatoryTyped[String](key), item)
    }).updateStateByKey[DataItem]((values: Seq[DataItem], state: Option[DataItem]) => {
      updateState(values, state)
    }).map(pair => {
      val item = pair._2

      item.put("key", pair._1)

      item
    })
  }

  def updateState(values: Seq[DataItem], state: Option[DataItem]): Option[DataItem] = {
    val state_item = state match {
      case Some(x: DataItem) => x
      case _ => new DataItem()
    }

    values.foreach(value => {
      summations.foreach(si => {
        value.getTyped[String](si.path) match {
          // summarize selected item
          case Some(v) => {
            // just keeping the last item for now
            state_item.put(si.destination, v)
          }

          case _ => None
        }
      })
    })

    Some(state_item)
  }
}

case class SumItem(val source: String,
                   val destination: String,
                   val function: String,
                   val retention: Int) {
  val path = DataItem.toPathElements(source)
}