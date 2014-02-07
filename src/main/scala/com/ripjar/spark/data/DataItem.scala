package com.ripjar.spark.data

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import com.github.nscala_time.time.Imports.DateTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import java.util.{ Map => JMap }
import java.util.{ List => JList }
import java.lang.{ String => JString }
import java.lang.{ Integer => JInteger }
import java.lang.{ Float => JFloat }
import java.lang.{ Double => JDouble }
import java.lang.{ Boolean => JBoolean }
import java.lang.{ Long => JLong }
import java.lang.{ String => JString }
import java.lang.{ Object => JObject }
import scala.collection.parallel.mutable

// all the DataItem representations must implement this trait.
trait DataItem extends Serializable {
  def getRaw(): Array[Byte]

  def get[T: ClassTag](key: ItemPath): Option[T]
  def getMandatory[T: ClassTag](key: ItemPath): T

  def put(key: ItemPath, value: Any)

  def remove(key: ItemPath): Option[Any]
}

// container for a pre-parsed path object.
class ItemPath(val key: String) extends Serializable {
  // for now we only have one representation
  var internal: List[String] = key.split("\\.").toList
}

object DataItem {
  val logger = LoggerFactory.getLogger("DataObject")

  /*def fromMap(mergeMap: Map[String, Any]): DataItem = {
    val di = new NestedDataItem
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
        case v: List[Double] => di.put(kv._1, new DoubleList(v.toList))
        case v: Array[String] => di.put(kv._1, new StringList(v.toList))
        case v: List[Map[String, Any]] => di.put(kv._1, new DataItemList(v.map(x => DataItem.fromMap(x))))
        case null => di.put(kv._1, "")

        case x: Any => logger.warn(kv._1, kv._2, x.toString)
        case d => throw new RuntimeException("Not implemented DataObject mapping from type: " + d)
      }
    })
    di
  }

  // TODO: this whole parsing needs to be reimplemented by manual walking of the object.
  def fromJson(json: String, template: DataItem = null, raw: Array[Byte] = null): DataItem = {
    // Because JSON does not handle longs well
    // TODO: probably need to do some proper parsing instead of this.
    val myNum = { input : String =>
      if(input.indexOf('.') > -1){
        java.lang.Double.parseDouble(input)
      } else {
        java.lang.Long.parseLong(input)
      }
    }

    scala.util.parsing.json.JSON.globalNumberParser = myNum
    scala.util.parsing.json.JSON.perThreadNumberParser = myNum

    scala.util.parsing.json.JSON.parseFull(json) match {
      case (Some(map: Map[String, Any])) => fromMap(map)
      case _ => new NestedDataItem
    }
  }*/

  def jsonToMap(json: String): Option[Any] = {
    val myNum = { input : String =>
      if(input.indexOf('.') > -1){
        java.lang.Double.parseDouble(input)
      } else {
        java.lang.Long.parseLong(input)
      }
    }

    scala.util.parsing.json.JSON.globalNumberParser = myNum
    scala.util.parsing.json.JSON.perThreadNumberParser = myNum

    scala.util.parsing.json.JSON.parseFull(json)
  }

  def fromJava(jm: JMap[JString, JObject]): DataItem = {
    val di = create()
    merge(di, jm)
    di
  }

  def merge(di: DataItem, jmap: JMap[JString, JObject]) {
    jmap.entrySet().foreach(entry => {
      val key = new ItemPath(entry.getKey)

      entry.getValue() match {
        case s: JString => di.put(key, s)
        case i: JInteger => di.put(key, i.intValue)
        case f: JFloat => di.put(key, f.floatValue)
        case d: JDouble => di.put(key, d.doubleValue)
        case b: JBoolean => di.put(key, b.booleanValue)
        case l: JLong => di.put(key, l.longValue)
        case m: JMap[JString, JObject] => {
          di.get(key) match {
            case Some(sm: DataItem) => merge(sm, m)
            case Some(_) => {
              DataItem.logger.info("Changing type on: %s".format(entry.getKey))
              di.put(key, DataItem.fromJava(m))
            }
            case None => {
              di.put(key, DataItem.fromJava(m))
            }
          }
        }
        case l: JList[JString] => di.put(key, l.map(_.toString).toList)
        case l: JList[JInteger] => di.put(key, l.map(_.intValue.toLong).toList)
        case l: JList[JFloat] => di.put(key, l.map(_.floatValue.toDouble).toList)
        case l: JList[JDouble] => di.put(key, l.map(_.doubleValue).toList)
        case l: JList[JBoolean] => di.put(key, l.map(_.booleanValue).toList)
        case l: JList[JLong] => di.put(key,l.map(_.longValue).toList)
        case l: JList[JMap[JString, JObject]] => di.put(key, l.map(jm => fromJava(jm)).toList)
        case _ => throw new RuntimeException("Not implmented DataObject mapping from type during merge: " + entry.getValue.getClass.getName)
      }
    })
  }

  def create(template: DataItem = null, raw: Array[Byte] = null): DataItem = {
    template match {
      case di: NestedDataItem => new NestedDataItem(raw)
      case di: HashDataItem => new HashDataItem(raw)
      case _ => new HashDataItem(raw)
    }
  }

  def fromMap(map: Map[String, Any], template: DataItem = null, raw: Array[Byte] = null): DataItem = {
    map.foldLeft[DataItem](create(template, raw))( (di, pair) => {
      val key = new ItemPath(pair._1)
      val value: Any = pair._2

      value match {
        case child: Map[String, Any] => di.put(key, fromMap(child))
        case v: Boolean => di.put(key, v)
        case v: Long => di.put(key, v)
        case v: Double => di.put(key, v)
        case v: String => di.put(key, v)
        case v: Array[Boolean] => di.put(key, v.toList)
        case v: Array[Long] => di.put(key, v.toList)
        case v: Array[Double] => di.put(key, v.toList)
        case v: List[Double] => di.put(key, v.toList)
        case v: Array[String] => di.put(key, v.toList)
        case v: List[Map[String, Any]] => {
          val items = v.map(obj => {
            fromMap(obj)
          })

          di.put(key, items)
        }
        case null => di.put(key, "")

        case x: Any => logger.warn(key.key, value, x.toString)
        case d => throw new RuntimeException("Not implemented DataObject mapping from type: " + d)
      }

      di
    })
  }

  def fromJSON(json: String, template: DataItem = null, raw: Array[Byte] = null): DataItem = {
    val map = jsonToMap(json) match {
      case (Some(maps: Map[String, Any])) => maps
      case _ => Map[String, Any]()
    }

    fromMap(map, template, raw)
  }
}

private class HashDataItem(val raw: Array[Byte]) extends HashMap[String, Any] with DataItem {

  def getRaw(): Array[Byte] = raw

  // convert to specific represenation of key
  def get[T: ClassTag](key: ItemPath): Option[T] = get[T](key.internal)

  def put(key: ItemPath, value: Any) {
    put(key.internal, value)
  }

  def get[T: ClassTag](key: List[String]): Option[T] = {
    key match {
      case Nil          => None // not found
      case head :: path => {
        get(head) match {
          case Some(di: HashDataItem) => di.get(path) // recursively search
          case Some(x: T)  => Some(x)
          case _           => None
        }
      }
    }
  }

  def getMandatory[T: ClassTag](key: ItemPath): T = {
    this.get[T](key) match {
      case Some(v: T) => v
      case _          => throw DataItemException("Field: '%s'.".format(key), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  def put(key: List[String], value: Any) {
    key match {
      case Nil => None
      case head :: Nil => {
        put(head, value)
      }

      case head :: path => {
        val next = get(head) match {
          case Some(di: HashDataItem) => di
          case _ => new HashDataItem(null)
        }

        next.put(path, value)
        put(head, value)
      }
    }
  }

  def remove(key: ItemPath): Option[Any] = {
    remove(key.internal)
  }

  def remove(key: List[String]): Option[Any] = {
    key match {
      case Nil         => None
      case head :: Nil => {
        remove(head)
      }
      case head :: path => {
        get(head) match {
          case Some(item: HashDataItem) => item.remove(path)
          case Some(x) => remove(head)
          case None    => None
        }
      }
    }
  }

  override def toString(): String = {
    new scala.util.parsing.json.JSONObject(this.toMap).toString
  }
}

//TODO: This interface gives the sort of functionality I think we want but performance may be an issue
// For now I'll leave as is 
// Simple flattening may work ... but an area of complexity is getting items from a list a.b.c[3].d.e
private class NestedDataItem(raw: Array[Byte] = null) extends DataItem {

  private val valueMap = new HashMap[String, Any]

  def getRaw(): Array[Byte] = raw

  def getMap(): Map[String, Any] = {
    valueMap.toMap
  }

  def put(key: String, value: List[Any]) = {
    valueMap.put(key, value)
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

  def remove(key: ItemPath): Option[Any] = {
    valueMap.remove(key.key)
  }

  def get(key: String): Option[Any] = {
    valueMap.get(key)
  }

  def get[T: ClassTag](key: ItemPath): Option[T] = {
    getTyped[T](key.internal)
  }

  def putItem(key: String, value: Any) = {
    put(key, value.toString)
  }

  def put(key: ItemPath, value: Any) = {
    put(key.key, value.toString)
  }

  def get(path: List[String]): Option[Any] = {
    path match {
      case h :: Nil => get(h)
      case h :: t => get(h) match {
        case Some(c: NestedDataItem) => c.get(t)
        case _ => None
      }
      case _ => None
    }
  }

  def getMandatory[T: ClassTag](key: ItemPath): T = {
    get[T](key) match {
      case Some(x: T) => x
      case None => throw DataItemException("Field: '%s'.".format(key), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  def getMandatory(path: List[String]): Any = {
    path match {
      case h :: Nil => getMandatory(new ItemPath(h))
      case h :: t => get(h) match {
        case Some(c: NestedDataItem) => c.getMandatory(t)
        case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
      }
      case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  def getTyped[T: ClassTag](key: String): Option[T] = {
    valueMap.get(key) match {
      case Some(v: T) => Some(v)
      case Some(x) => {
        DataItem.logger.warn("Found field '%s' of the wrong type: %s".format(key, x.getClass.getName()))
        None
      }
      case None => None
    }
  }
  def getTyped[T: ClassTag](path: List[String]): Option[T] = {
    path match {
      case Nil    => None
      case h :: t => get(h) match {
        case Some(subItem: NestedDataItem) => subItem.getTyped[T](t)
        case Some(item: T) => Some(item)
        case _ => None
      }
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
        case Some(c: NestedDataItem) => c.getMandatoryTyped[T](t)
        case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
      }
      case _ => throw DataItemException("Field: '%s'.".format(path.mkString), DataItemErrorType.CannotFindMandatoryField)
    }
  }

  override def toString(): String = {
    new scala.util.parsing.json.JSONObject(valueMap.toMap).toString
  }
}