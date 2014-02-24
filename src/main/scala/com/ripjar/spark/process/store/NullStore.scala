package com.ripjar.spark.process.store

import com.ripjar.spark.job.InstanceConfig
import com.ripjar.spark.process.{TerminalProcessor, Processor}
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.{ItemPath, DataItem}
import org.slf4j.LoggerFactory

/**
 * Created by mike on 2/23/14.
 */
object NullStore {
  val logger = LoggerFactory.getLogger(classOf[NullStore])
}

class NullStore(config: InstanceConfig) extends TerminalProcessor {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.foreachRDD( rdd => {
      NullStore.logger.info("Did nothing with " + rdd.count() + " records")
    })

    stream
  }
}
