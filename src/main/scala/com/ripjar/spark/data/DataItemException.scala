package com.ripjar.spark.data

object DataItemErrorType extends Enumeration {
  val GetCalledOnWrongDatatype, CannotFindMandatoryField = Value
}

case class DataItemException(message: String, errorType: DataItemErrorType.Value) extends RuntimeException(errorType + ": " + message)
 