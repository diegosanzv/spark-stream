package com.ripjar.spark.data

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import com.github.nscala_time.time.Imports.DateTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

object DataItem {
  val logger = LoggerFactory.getLogger("DataObject")

  val PATH_SEPARATOR = "\\."

  def toPathElements(path: String): List[String] = {
    path.split(PATH_SEPARATOR).toList
  }

  def fromMap(mergeMap: Map[String, Any]): DataItem = {
    val di = new DataItem
    mergeMap.foreach(kv => {
      kv._2 match {
        case child: Map[String, Any] => di.put(kv._1, DataItem.fromMap(child))
        case v: Boolean => di.put(kv._1, v)
        case v: Long => di.put(kv._1, v)
        case v: Double => di.put(kv._1, v)
        case v: String => di.put(kv._1, v)
        case v: Array[Boolean] => di.put(kv._1, new BooleanList(v.toList))
        case v: Array[Long] => di.put(kv._1, new LongList(v.toList))
        case v: Array[Double] => di.put(kv._1, new DoubleList(v.toList))
        case v: Array[String] => di.put(kv._1, new StringList(v.toList))
        case v: List[Map[String, Any]] => di.put(kv._1, new DataItemList(v.map(x => DataItem.fromMap(x))))

        case x: Any => logger.warn(kv._1, kv._2, x.toString)
        case _ => throw new RuntimeException("Not implmented DataObject mapping from type")
      }
    })
    di
  }

  def fromJson(json: String): DataItem = {
    scala.util.parsing.json.JSON.parseFull(json) match {
      case (Some(map: Map[String, Any])) => fromMap(map)
      case _ => new DataItem
    }
  }
}

//TODO: This interface gives the sort of functionality I think we want but performance may be an issue
// For now I'll leave as is 
// Simple flattening may work ... but an area of complexity is getting items from a list a.b.c[3].d.e
class DataItem() extends Serializable {

  var raw: Array[Byte] = null
  private val valueMap = new HashMap[String, Any]

  def getMap(): Map[String, Any] = {
    valueMap.toMap
  }

  def put(key: String, value: DataList): Unit = {
    valueMap.put(key, value)
  }

  def put(key: String, value: DataItem): Unit = {
    valueMap.put(key, value)
  }

  def put(key: String, value: String): Unit = {
    valueMap.put(key, value)
  }
  def put(key: String, value: Boolean): Unit = {
    valueMap.put(key, value)
  }
  def put(key: String, value: Long): Unit = {
    valueMap.put(key, value)
  }
  def put(key: String, value: Double): Unit = {
    valueMap.put(key, value)
  }
  def put(key: String, value: DateTime): Unit = {
    valueMap.put(key, value)
  }

  def remove(key: String): Unit = {
    valueMap.remove(key)
  }

  def get(key: String): Option[Any] = {
    valueMap.get(key)
  }
  def get(path: List[String]): Option[Any] = {
    path match {
      case h :: Nil => get(h)
      case h :: t => get(h) match {
        case Some(c: DataItem) => c.get(t)
        case _ => None
      }
      case _ => None
    }
  }

  def getMandatory(key: String): Any = {
    get(key) match {
      case Some(x) => x
      case None => throw DataItemException("Field: '%s'.".format(key), DataItemErrorType.CannotFindMandatoryField)
    }
  }
  def getMandatory(path: List[String]): Any = {
    path match {
      case h :: Nil => getMandatory(h)
      case h :: t => get(h) match {
        case Some(c: DataItem) => c.getMandatory(t)
        case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
      }
      case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  def getTyped[T: ClassTag](key: String): Option[T] = {
    valueMap.get(key) match {
      case Some(v: T) => Some(v)
      case Some(x) => {
        DataItem.logger.warn("Found field '%s' of the wrong type: %s".format(key, x.getClass.getName()));
        None
      }
      case None => None
    }
  }
  def getTyped[T: ClassTag](path: List[String]): Option[T] = {
    path match {
      case h :: Nil => getTyped[T](h)
      case h :: t => get(h) match {
        case Some(c: DataItem) => c.getTyped[T](t)
        case _ => None
      }
      case _ => None
    }
  }

  def getMandatoryTyped[T: ClassTag](key: String): T = {
    getTyped[T](key) match {
      case Some(x) => x.asInstanceOf[T]
      case None => throw DataItemException("Field: '%s'.".format(key), DataItemErrorType.CannotFindMandatoryField)
    }
  }
  def getMandatoryTyped[T: ClassTag](path: List[String]): T = {
    path match {
      case h :: Nil => getMandatoryTyped[T](h)
      case h :: t => get(h) match {
        case Some(c: DataItem) => c.getMandatoryTyped[T](t)
        case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
      }
      case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  override def toString(): String = {
    new scala.util.parsing.json.JSONObject(valueMap.toMap).toString
  }

  private def parsePath(path: String): (List[String], String) = {
    val paths = path.split(DataItem.PATH_SEPARATOR)
    (paths.slice(0, paths.length - 1).toList, paths(paths.length - 1))
  }

}