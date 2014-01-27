package com.ripjar.spark.job


object SparkJobErrorType extends Enumeration {
  val NoProcessAfterNormalization, NoProcessorForType, NoProcessAfterIngest, InvalidConfig, ConfigParameterParseError, RecursiveRouteError, MandatoryParameterNotPresent, ProcessingError, JsonInflateError = Value
}
case class SparkJobException(message: String, errorType: SparkJobErrorType.Value) extends RuntimeException(errorType + ": " + message)