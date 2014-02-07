package com.ripjar.spark.process

import com.ripjar.spark.data._
import com.ripjar.spark.job.SparkJobException
import com.ripjar.spark.job.SparkJobErrorType
import org.apache.tika.Tika
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import org.apache.tika.metadata.Metadata
import com.ripjar.spark.job.InstanceConfig

object TextExtract {
  val logger = LoggerFactory.getLogger(classOf[TextExtract])

  //TODO: Need to investigate whether this is the right way to do this
  var tika: Tika = null
  var lock = ""

  def getTika(): Tika = {
    if (tika == null) {
      lock.synchronized {
        if (tika == null) {
          tika = new Tika
        }
      }
    }
    tika
  }

  val mapping: Map[String, String] = Map(
    "acknowledgement" -> "acknowledgement",
    "application-name" -> "application-name",
    "application-version" -> "application-version",
    "author" -> "author",
    "category" -> "category",
    "cmd_ln" -> "cmd_ln",
    "comment" -> "comment",
    "comments" -> "comments",
    "company" -> "company",
    "contact" -> "contact",
    "content-disposition" -> "content-disposition",
    "content-encoding" -> "content-encoding",
    "content-language" -> "content-language",
    "content-length" -> "content-length",
    "content-location" -> "content-location",
    "content-md5" -> "content-md5",
    "content-status" -> "content-status",
    "content-type" -> "content-type",
    "contributor" -> "contributor",
    "conventions" -> "conventions",
    "coverage" -> "coverage",
    "creation-date" -> "creation-date",
    "creator" -> "creator",
    "dc:creator" -> "creator",
    "dcterms:created" -> "creation-date",
    "description" -> "description",
    "edit-time" -> "edit-time",
    "experiment_id" -> "experiment_id",
    "format" -> "format",
    "history" -> "history",
    "identifier" -> "identifier",
    "institution" -> "institution",
    "keywords" -> "keywords",
    "language" -> "language",
    "last-author" -> "last-author",
    "license-location" -> "license-location",
    "license-url" -> "license-url",
    "location" -> "location",
    "manager" -> "manager",
    "message-bcc" -> "message-bcc",
    "message-cc" -> "message-cc",
    "message-from" -> "message-from",
    "message-recipient-address" -> "message-recipient-address",
    "message-to" -> "message-to",
    "model_name_english" -> "model_name_english",
    "modified" -> "modified",
    "notes" -> "notes",
    "presentation-format" -> "presentation-format",
    "prg_id" -> "prg_id",
    "project_id" -> "project_id",
    "publisher" -> "publisher",
    "realization" -> "realization",
    "references" -> "references",
    "relation" -> "relation",
    "revision-number" -> "revision-number",
    "rights" -> "rights",
    "security" -> "security",
    "source" -> "source",
    "source" -> "source",
    "subject" -> "subject",
    "table_id" -> "table_id",
    "template" -> "template",
    "title" -> "title",
    "total-time" -> "total-time",
    "type" -> "type",
    "version" -> "version",
    "work-type" -> "work-type")

}

/*
 * Text extract the stream
 * 
 * Config parameters: 
 *  	none
 * 
 * Task parameters:
 *
 */
class TextExtract(config: InstanceConfig) extends Processor with Serializable {

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.map(extract(_))
  }

  def extract(input: DataItem): DataItem = {
    if (input.getRaw() != null) {
      extract(input.getRaw(), input)
    }
    input
  }

  def extract(raw: Array[Byte], dataset: DataItem): Unit = {

    val result = extract(new ByteArrayInputStream(raw))

    val metadata = DataItem.create(null)
    val extras = DataItem.create(null)

    result._2.map(mt => metadata.put(new ItemPath(mt._1), mt._2))
    result._3.map(mt => extras.put(new ItemPath(mt._1), mt._2))

    dataset.put(new ItemPath("text"), result._1)
    dataset.put(new ItemPath("metadata"), metadata)
    dataset.put(new ItemPath("text_extra"), extras)
  }

  private def extract(stream: InputStream): (String, Map[String, String], Map[String, String]) = {
    def parseMetadata(metadata: Metadata): (Map[String, String], Map[String, String]) = {
      def getPname(uname: String): String = {
        val lname = uname.toLowerCase
        if (lname.startsWith("meta:")) {
          lname.substring(5)
        } else {
          lname
        }
      }
      val metaMap = scala.collection.mutable.HashMap[String, String]()
      val extraMap = scala.collection.mutable.HashMap[String, String]()

      for (name <- metadata.names) {
        val value = metadata.get(name).trim
        if (!value.isEmpty) {
          val pname = getPname(name)
          TextExtract.mapping.get(pname) match {
            case Some(n) => metaMap.put(n, value)
            case None => extraMap.put(name, value)
          }
        }
      }

      val iMetaMap = collection.immutable.HashMap() ++ metaMap
      val iExtraMap = collection.immutable.HashMap() ++ extraMap

      (iMetaMap, iExtraMap)
    }

    val metadata: Metadata = new Metadata()
    val s = TextExtract.getTika.parseToString(stream, metadata)
    stream.close

    val m = parseMetadata(metadata)

    (s, m._1, m._2)
  }

}