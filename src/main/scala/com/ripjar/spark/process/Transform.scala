package com.ripjar.spark.process

import com.ripjar.spark.data._
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.job.{SparkJobErrorType, SparkJobException, InstanceConfig}
import org.json4s.JsonAST.{JString, JArray, JValue}
import org.json4s.DefaultFormats

/*
 * Performs simple movements and merges in the map based upon task parameters
 * Maybe should be merged with filter
 * 
 * Config parameters: 
 *  	None
 * 
 * Task parameters:
 * 		"transform": 
 *   		[
 *     			{"op": "move", "from": "xxx", "to": "yyy"},
 *   			  {"op": "copy", "from": "xxx", "to": "yyy"}
 *   			  {"op": "filter", "filter_in": true, "paths": ["path list"]}
 *   			  {"op": "delete", "paths": ["path list"]}
 *   			  {"op": "flatten", "path": "path.path"}
 *   			  {"op": "str_join", "path": "path to list", "separator": "separator"}
 *   			  {"op": "str_split", "path": "path to string", "regex": "split on this"}
 *      ]
 *
 */
class Transform(config: InstanceConfig) extends Processor with Serializable {
  val ops = load_ops(config.data)

  private def load_ops(data: JValue): List[Transformer] = {
    implicit val formats = DefaultFormats

    data match {
      case arr: JArray => {
        arr.children.map(opItem => {
          (opItem \ "op").asInstanceOf[JString].values match {
            case "move" => opItem.extract[Move]
            case "copy" => opItem.extract[Copy]
            case "flatten" => opItem.extract[Flatten]
            case "filter" => opItem.extract[TransFilter]
            case "delete" => opItem.extract[Delete]
            case "str_split" => opItem.extract[SplitStr]
            case "str_join" => opItem.extract[JoinStr]
            case str => throw new SparkJobException("Unsupported transformation operation: " + str, SparkJobErrorType.InvalidConfig)
          }
        })
      }

      case _ => {
        throw new SparkJobException("Expecting an array in transformer data", SparkJobErrorType.InvalidConfig)
      }
    }
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.flatMap(item => {
      ops.foldLeft[List[DataItem]](List(item))( (curList, op) => {
        op.apply(curList)
      })
    })
  }
}

trait Transformer {
  def apply(items: List[DataItem]): List[DataItem]
}

case class Move(from: String, to: String) extends Transformer {
  val fromPath = new ItemPath(from)
  val toPath = new ItemPath(to)

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      item.remove(fromPath) match {
        case Some(x) => {
          item.put(toPath, x)
          item
        }

        case None => item
      }
    })
  }
}

case class Copy(from: String, to: String) extends Transformer {
  val fromPath = new ItemPath(from)
  val toPath = new ItemPath(to)

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      item.get[Any](fromPath) match {
        case Some(x) => item.put(toPath, x)
        case None => null
      }

      item
    })
  }
}

case class Flatten(path: String) extends Transformer {
  val fpath = new ItemPath(path)

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.flatMap(item => {
      item.remove(fpath) match {
        case Some(v: List[Any]) => {
          v.map(lst_item => {
            val newDi = item.copy()

            newDi.put(fpath, lst_item)
            newDi
          })
        }

        case Some(v) => {
          item.put(fpath, v)
          item :: Nil
        }

        case None => {
          item :: Nil
        }
      }
    })
  }
}

case class TransFilter(filter_in: Boolean, paths: List[String]) extends Transformer {
  val item_paths = paths.map(p => new ItemPath(p))

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      filter_in match {
        case true  => filter_in(item)
        case false => filter_out(item)
      }
    })
  }

  def filter_in(item: DataItem): DataItem = {
    val newItem = DataItem.create(item)

    item_paths.foreach(path => {
      item.get[Any](path) match {
        case Some(d) => newItem.put(path, d)
        case None => null
      }
    })

    newItem
  }

  def filter_out(item: DataItem): DataItem = {
    item_paths.foreach(path => {
      item.remove(path)
    })

    item
  }
}

case class Delete(paths: List[String]) extends Transformer {
  val item_paths = paths.map(p => new ItemPath(p))

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      item_paths.foreach(path => {
        item.remove(path)
      })

      item
    })
  }
}

case class SplitStr(path: String, regex: String) extends Transformer {
  val split_path = new ItemPath(path)

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      item.get[String](split_path) match {
        case Some(str) => item.put(split_path, str.split(regex).toList)
        case None => {}
      }

      item
    })
  }
}

case class JoinStr(path: String, separator: String) extends Transformer {
  val join_path = new ItemPath(path)

  override def apply(items: List[DataItem]): List[DataItem] = {
    items.map(item => {
      item.get[List[String]](join_path) match {
        case Some(lst: List[String]) => item.put(join_path, lst.mkString(separator))
        case None => {}
      }

      item
    })
  }
}