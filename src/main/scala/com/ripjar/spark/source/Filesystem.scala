package com.ripjar.spark.source

import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.DataItem
import com.ripjar.spark.job.SourceConfig
import org.apache.spark.streaming.StreamingContext

/**
 * Created by mike on 2/4/14.
 */
class Filesystem(config: SourceConfig, ssc: StreamingContext) extends Source {
  val dir: String = config.getMandatoryParameter("dir")

  def stream() : DStream[DataItem] = {
    ssc.textFileStream(dir).map( (json: String) => {
      DataItem.fromJson(json)
    })
  }
}
