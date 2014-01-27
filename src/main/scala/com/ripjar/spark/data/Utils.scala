package com.ripjar.spark.data

object Utils {
  
  scala.util.parsing.json.JSON.globalNumberParser = globalNumberParserFunction

  def globalNumberParserFunction(input: String) = {
    try {
      input.toLong
    } catch {
      case e: Exception => {
        try {
          input.toDouble
        } catch {
          case e: Exception => {
            input
          }
        }

      }
    }
  }
}