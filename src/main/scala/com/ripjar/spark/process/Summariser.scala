package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.{SparkJobErrorType, SparkJobException, InstanceConfig}
import org.apache.spark.streaming.StreamingContext._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import scala.reflect.ClassTag

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
  val key = new ItemPath(config.getMandatoryParameter("unique_key"))
  val summations = parseData(config.data)

  private def parseData(data: JValue): Array[SumItem] = {
    implicit val formats = DefaultFormats

    data.extract[Array[SumItem]]
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    val key_path = new ItemPath("key")

    stream.map(item => {
      (item.getMandatory[String](key), item)
    }).updateStateByKey[DataItem]( (values: Seq[DataItem], state: Option[DataItem]) => {
      updateState(values, state)
    }).map(pair => {
      val item = pair._2

      item.put(key_path, pair._1)

      item
    })
  }

  def updateState(values: Seq[DataItem], state: Option[DataItem]): Option[DataItem] = {
    val start_state: DataItem = state match {
      case Some(x: DataItem) => x
      case _ => DataItem.create()
    }

    val end_state = values.foldLeft[DataItem](start_state)( (v_state, value) => {
      summations.foldLeft[DataItem](v_state)( (s_state, si) => {
        si.function match {
          case "track" => track(si, value, s_state)
          case "set"   => set(si, value, s_state)
          case "stat"  => stat(si, value, s_state)
          case f       => throw new SparkJobException("Unsupported summarization function: " + f, SparkJobErrorType.InvalidConfig)
        }
      })
    })

    Some(end_state)
  }

  // summarization functions
  //
  def basic_summary[U: ClassTag, T: Manifest](si: SumItem, value: DataItem, state: DataItem, func: ((U, T) => T), newState: () => T): DataItem = {
    value.get[U](si.source_path) match {
      case Some(v) => {
        val tracked: T = state.get[T](si.destination_path) match {
          case Some(x: T) => x
          case z       => newState() // for some reason manifest doesn't work with lists.
        }

        state.put(si.destination_path, func(v, tracked))
      }

      case _ => None
    }

    state
  }

  def track(si: SumItem, value: DataItem, state: DataItem): DataItem = {
    basic_summary[String, List[String]](si, value, state, (s_val, d_val) => {
      (s_val :: d_val).take(si.retention)
    }, () => {
      List[String]()
    })
  }

  def set(si: SumItem, value: DataItem, state: DataItem): DataItem = {
    basic_summary[String, List[String]](si, value, state, (s_val, d_val) => {
      if(!d_val.contains(s_val)) {
        (s_val :: d_val).take(si.retention)
      } else {
        d_val
      }
    }, () => {
      List[String]()
    })
  }

  val sumPath = new ItemPath("sum")
  val countPath = new ItemPath("count")
  val maxPath = new ItemPath("max")
  val minPath = new ItemPath("min")

  def stat(si: SumItem, value: DataItem, state: DataItem): DataItem = {
    basic_summary[Number, DataItem](si, value, state, (src_val, d_val) => {
      val s_val : Double = if(src_val.isInstanceOf[Double]) {
        src_val.asInstanceOf[Double]
      } else if(src_val.isInstanceOf[Long]) {
        src_val.asInstanceOf[Long].toDouble
      } else {
        return d_val // unsupported type
      }

      d_val.put(sumPath, d_val.get[Double](sumPath) match {
        case Some(s) => s + s_val
        case _       => 0.0
      })

      d_val.put(countPath, d_val.get[Int](countPath) match {
        case Some(s: Int) => s + 1
        case _       => 0
      })

      d_val.put(maxPath, d_val.get[Double](maxPath) match {
        case Some(s) => Math.max(s, s_val)
        case _       => Double.MinValue
      })

      d_val.put(minPath, d_val.get[Double](minPath) match {
        case Some(s) => Math.min(s, s_val)
        case _       => Double.MaxValue
      })

      d_val
    }, () => {
      DataItem.create(state)
    })
  }
}

case class SumItem(val source: String,
                   val destination: String,
                   val function: String,
                   val retention: Int) {
  val source_path = new ItemPath(source)
  val destination_path = new ItemPath(destination)
}