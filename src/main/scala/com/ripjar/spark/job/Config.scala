package com.ripjar.spark.job

import org.json4s._
import org.json4s.native.JsonMethods._
import scala.Some
import java.io.File
import scala.reflect.ClassTag

object Config {
  def parseJsonFile(jsonFile: File): StreamConfig = {
    val json = try {
      scala.io.Source.fromFile(jsonFile).mkString
    } catch {
      case e: Exception => throw new SparkJobException("Cannot read JSON config file: %s".format(jsonFile.getName), SparkJobErrorType.InvalidConfig)
    }
    val jvRoot: JValue = parse(json)

    jvRoot match {
      case joRoot: JObject => {
        extractObject(joRoot)
      }
      case _ => {
        throw new SparkJobException("Cannot parse the JSON config file: ".format(jsonFile.getName), SparkJobErrorType.InvalidConfig)

      }
    }
  }

  def extractObject(joRoot: JObject) : StreamConfig = {
    implicit val formats = DefaultFormats

    val sysConfig = (joRoot \ "system").extract[SystemConfig]
    val procs     = extractParam[ProcessorConfig]( (joRoot \ "processors") )
    val insts     = extractParam[InstanceConfig]( (joRoot \ "instances") )
    val flows     = extractParam[FlowConfig]( (joRoot \ "flows") )
    val srcs      = extractParam[SourceConfig]( (joRoot \ "sources") )

    new StreamConfig(sysConfig, procs, insts, flows, srcs)
  }

  def extractParam[T:Manifest](root: JValue): Array[T] = {
    implicit val formats = DefaultFormats

    var lst = List[T]()

    root match {
      case arr: JArray => {
        for(item <- arr.children) {
          val config = item.extract[T]

          if(config.isInstanceOf[AbstractParameterizedConfig]) {
            val need_data = config.asInstanceOf[AbstractParameterizedConfig]
            val data = item \ "data"

            need_data.data = data
          }

          lst = config :: lst
        }
      }

      case _ =>
        throw new SparkJobException("Cannot parse the JSON config, Array expected (" + root + ")", SparkJobErrorType.InvalidConfig)
    }

    lst.reverse.toArray[T]
  }
}

abstract class AbstractParameterizedConfig(val parameters: Map[String, String]) { //Parameters is optional
  var data: JValue = null

  def getMandatoryParameter(key: String): String = {
    parameters.get(key) match {
      case Some(x) => x
      case None => throw new SparkJobException("Missing: '%s', found: %s".format(key, parameters.mkString("[", ",", "]")), SparkJobErrorType.MandatoryParameterNotPresent)
    }
  }

  def getParameter(key: String, default: String) : String = {
    parameters.get(key) match {
      case Some(x) => x
      case None => default
    }
  }
}

case class StreamConfig(
  val system: SystemConfig,
  val processors: Array[ProcessorConfig],
  val instances: Array[InstanceConfig],
  val flows: Array[FlowConfig],
  val sources: Array[SourceConfig]) {}

case class SystemConfig(val task_name: String,
                        val spark_master: String,
                        val spark_home: String,
                        val stream_duration: Int,
                        val jars: Array[String]) {}

case class ProcessorConfig(val id: String,
  val classname: String,
  val jars: Array[String] = Array.empty, //No need to pass jars that will be in the classpath already
  val datasets: Array[String] = Array.empty, // Dataset is optional
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "classname: %s, jars: %s, datasets: %s, parameters: %s".format(classname, jars.mkString("[", ",", "]"), datasets.mkString("[", ",", "]"), parameters.mkString("[", ",", "]"))

}

//An instance of a processor
case class InstanceConfig(
  val id: String,
  val processId: String,
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "id: %s, processId: %s, parameters: %s".format(id, processId, parameters.mkString("[", ",", "]"))
}

case class FlowConfig(val id: String,
  val sequence: Array[String]) {
  override def toString() = "sequence: %s".format(sequence.mkString("[", ",", "]"))
}

case class SourceConfig(
  val id: String,
  val classname: String,
  val jars: Array[String] = Array.empty, //No need to pass jars that will be in the classpath already
  val datasets: Array[String] = Array.empty, // Dataset is optional
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "id: %s, classname: %s, jars: %s, datasets: %s, parameters: %s".format(id, classname, jars.mkString("[", ",", "]"), datasets.mkString("[", ",", "]"), parameters.mkString("[", ",", "]"))

}
