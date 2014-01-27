package com.ripjar.spark.job

abstract class AbstractParameterizedConfig(val parameters: Map[String, String]) {
  def getMandatoryParameter(key: String): String = {
    parameters.get(key) match {
      case Some(x) => x
      case None => throw SparkJobException("Missing: '%s'.".format(key), SparkJobErrorType.MandatoryParameterNotPresent)
    }
  }
}

case class IngestConfig(
  val id: String,
  val ingestType: String,
  override val parameters: Map[String, String],
  val datasets: Array[String]) extends AbstractParameterizedConfig(parameters) {}

case class ProcessConfig(val id: String,
  val from: String,
  val processType: String,
  override val parameters: Map[String, String],
  val datasets: Array[String]) extends AbstractParameterizedConfig(parameters) {}

case class Config(val client: String,
  val api: String,
  val id: String,
  val task_name: String,
  val spark_master: String,
  val spark_home: String,
  val stream_duration: Int,
  val jars: Array[String],
  val from: IngestConfig,
  val processes: Array[ProcessConfig]) {}

case class StreamConfig(val client: String,
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
  val jar: String,
  val parameters: Map[String, String],
  val datasets: Array[String]) {}

case class Instance(val id: String,
  val processId: String,
  override val parameters: Map[String, String]) extends AbstractParameterizedConfig(parameters) {}


case class Flow(val id: String,
  val sequence: Array[String]) {}

case class SourceCfg(val id: String,
  val classname: String,
  override val parameters: Map[String, String],
  val datasets: Array[String]) extends AbstractParameterizedConfig(parameters) {}
