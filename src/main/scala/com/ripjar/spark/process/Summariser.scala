package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.{SparkJobErrorType, SparkJobException, InstanceConfig}
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
  val key = config.getMandatoryParameter("unique_key")
  val summations = parseData(config.data)

  private def parseData(data: JValue): Array[SumItem] = {
    implicit val formats = DefaultFormats

    data.extract[Array[SumItem]]
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(item => {
      (item.getMandatory[String](new ItemPath(key)), item)
    }).updateStateByKey[DataItem]((values: Seq[DataItem], state: Option[DataItem]) => {
      updateState(values, state)
    }).map(pair => {
      val item = pair._2

      item.put(new ItemPath("key"), pair._1)

      item
    })
  }

  def updateState(values: Seq[DataItem], state: Option[DataItem]): Option[DataItem] = {
    val state_item: DataItem = state match {
      case Some(x: DataItem) => x
      case _ => DataItem.create(null)
    }

    values.foreach(value => {
      summations.foreach(si => {
        si.function match {
          case "track" => track(si, value, state_item)
          case f       => throw new SparkJobException("Unsupported summarization function: " + f, SparkJobErrorType.InvalidConfig)
        }
      })
    })

    Some(state_item)
  }

  def track(si: SumItem, value: DataItem, state: DataItem): DataItem = {
    val trackedList: List[Any] = state.get[List[String]](new ItemPath(si.destination)) match {
      case Some(x) => x
      case _       => List[Any]()
    }

    value.get[String](new ItemPath(si.source)) match {
      case Some(v) => {
        state.put(new ItemPath(si.destination), v :: trackedList)
      }

      case _ => None
    }

    state
  }
}

case class SumItem(val source: String,
                   val destination: String,
                   val function: String,
                   val retention: Int) {
  val path = DataItem.toPathElements(source)
}