package com.ripjar.spark.process

import com.ripjar.spark.job.InstanceConfig
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.{ItemPath, DataItem}

/**
 * Created by mike on 2/23/14.
 */
class TwitterTagExtractor(config: InstanceConfig) extends Processor with Serializable {
  val inputPath = new ItemPath(config.getParameter("input", "dataset.tweet.entities.hashtags"))
  val outputPath = new ItemPath(config.getParameter("output", "tag"))

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    val tag_indeces_path = new ItemPath("indices")
    val tag_text_path = new ItemPath("text")

    stream.filter( item => {
      // confirm we have the tags
      item.get[List[DataItem]](inputPath) match {
        case Some(x) => x.length > 0
        case None => false
      }

    }).flatMap( item => {
      // Make the data items
      item.getMandatory[List[DataItem]](inputPath).map(tag_item => {
        tag_item.remove(tag_indeces_path)

        tag_item.remove(tag_text_path) match {
          case Some(tag) => tag_item.put(outputPath, tag)
          case _ => null
        }

        tag_item
      })
    })
  }
}
