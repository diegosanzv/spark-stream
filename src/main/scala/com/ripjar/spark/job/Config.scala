package com.ripjar.spark.job

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

//TODO: How would we specify a tee. Where name in sequence matches, there the split happens.
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
