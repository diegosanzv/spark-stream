package com.ripjar.spark.job

import org.json4s._
import com.ripjar.spark.job.SourceCfg
import com.ripjar.spark.job.ProcessorCfg
import com.ripjar.spark.job.StreamConfig
import com.ripjar.spark.job.AppConfig
import com.ripjar.spark.job.Flow
import com.ripjar.spark.job.Instance
import scala.Some
import com.ripjar.spark.job.SparkJobException
import org.json4s.native.JsonMethods._
import com.ripjar.spark.job.SourceCfg
import com.ripjar.spark.job.ProcessorCfg
import com.ripjar.spark.job.StreamConfig
import com.ripjar.spark.job.AppConfig
import com.ripjar.spark.job.Flow
import com.ripjar.spark.job.Instance
import scala.Some
import com.ripjar.spark.job.SparkJobException
import com.ripjar.spark.job.SourceCfg
import com.ripjar.spark.job.ProcessorCfg
import com.ripjar.spark.job.StreamConfig
import com.ripjar.spark.job.AppConfig
import com.ripjar.spark.job.Flow
import com.ripjar.spark.job.Instance
import scala.Some
import com.ripjar.spark.job.SparkJobException

object Config {
  def parseJsonFile(jsonFile: String): StreamConfig = {
    val json = try {
      scala.io.Source.fromFile(jsonFile).mkString
    } catch {
      case e: Exception => throw new SparkJobException("Cannot read JSON config file: %s".format(config.configFile.getName), SparkJobErrorType.InvalidConfig)
    }
    val jvRoot: JValue = parse(json)

    jvRoot match {
      case joRoot: JObject => {
        implicit val formats = DefaultFormats
        joRoot.extract[StreamConfig]
      }
      case _ => {
        throw new SparkJobException("Cannot parse the JSON config file: ".format(config.configFile.getName), SparkJobErrorType.InvalidConfig)

      }
    }
  }
}

abstract class AbstractParameterizedConfig(val parameters: Map[String, String]) { //Parameters is optional
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
  val client: String,
  val api: String,
  val id: String,
  val task_name: String,
  val spark_master: String,
  val spark_home: String,
  val stream_duration: Int,
  val jars: Array[String],
  val processors: Array[ProcessorCfg],
  val instances: Array[Instance],
  val flows: Array[Flow],
  val sources: Array[SourceCfg]) {}

case class ProcessorCfg(val id: String,
  val classname: String,
  val jars: Array[String] = Array.empty, //No need to pass jars that will be in the classpath already
  val datasets: Array[String] = Array.empty, // Dataset is optional
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "classname: %s, jars: %s, datasets: %s, parameters: %s".format(classname, jars.mkString("[", ",", "]"), datasets.mkString("[", ",", "]"), parameters.mkString("[", ",", "]"))

}

//An instance of a processor
case class Instance(
  val id: String,
  val processId: String,
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "id: %s, processId: %s, parameters: %s".format(id, processId, parameters.mkString("[", ",", "]"))
}

case class Flow(val id: String,
  val sequence: Array[String]) {
  override def toString() = "sequence: %s".format(sequence.mkString("[", ",", "]"))
}

case class SourceCfg(
  val id: String,
  val classname: String,
  val jars: Array[String] = Array.empty, //No need to pass jars that will be in the classpath already
  val datasets: Array[String] = Array.empty, // Dataset is optional
  override val parameters: Map[String, String] = Map.empty) extends AbstractParameterizedConfig(parameters) {
  override def toString() = "id: %s, classname: %s, jars: %s, datasets: %s, parameters: %s".format(id, classname, jars.mkString("[", ",", "]"), datasets.mkString("[", ",", "]"), parameters.mkString("[", ",", "]"))

}
