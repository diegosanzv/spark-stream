package com.ripjar.spark.job

import java.io.File
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream.DStream
import com.ripjar.spark.data.DataItem
import com.ripjar.spark.process.Processor
import com.ripjar.spark.source.Source
import org.slf4j.Logger
import org.slf4j.LoggerFactory

case class AppConfig(configFile: File = new File("."), start: Boolean = false, stop: Boolean = false, name: String = "")

object App {

  val logger = LoggerFactory.getLogger(classOf[App])

  private val parser = new scopt.OptionParser[AppConfig]("sparkJob") {
    head("sparkJob", "1.0")
    opt[File]('c', "config") valueName ("<config file>") action { (x, c) =>
      c.copy(configFile = x)
    } text ("config file")
    opt[Unit]("start") action { (_, c) =>
      c.copy(start = true)
    } text ("start this config")
    opt[Unit]("stop") action { (_, c) =>
      c.copy(stop = true)
    } text ("stop names route")
    opt[String]("name") action { (x, c) =>
      c.copy(name = x)
    } text ("route name")
  }

  //TODO: Start / Stop param needed
  def main(args: Array[String]) {
    println(">>>>>>>>>>>>>>>>>>" + args.mkString(","))
    parser.parse(args, AppConfig()) map { appconfig =>
      if (appconfig.start) {
        val config = parseJsonFile(appconfig)

        System.getProperties.setProperty("spark.cleaner.ttl", "7200")

        println("config.spark_master: " + config.spark_master)

        val ssc = new StreamingContext(config.spark_master, config.task_name, Seconds(config.stream_duration), config.spark_home, config.jars)
        ssc.checkpoint("checkpoint")

        // build and start the job
        buildProcessing(config, ssc)

        ssc.start()
      } else if (appconfig.stop) {
        //TODO: STOP
      } else {
        System.err.println("Need to run with either --start or --stop.")
        parser.showUsage
      }
    } getOrElse {
      System.err.println("Cannot parse the command arguments")
      System.exit(1)
    }
  }

  def buildProcessing(config: StreamConfig, ssc: StreamingContext) {
    // read the processor configs
    val processors: Map[String, Any] = config.processors.map(proc => {
      (proc, createClass[com.ripjar.spark.process.Processor](proc.classname))

    }).foldLeft(Map[String, Any]())((map: Map[String, Any], item: (ProcessorCfg, Any)) => {
      map ++ Map(item._1.id -> item)
    })

    // instantiate processors
    val instances: Map[String, Any] = config.instances.map(inst => {
      println("Instance: " + inst.toString)
      val (procConfig: ProcessorCfg, klass: Class[Processor]) = processors.get(inst.processId) match {
        case Some(id) => id
        case _ => throw new SparkJobException("Cannot find a process with id %s, required for instance %s.".format(inst.processId, inst.id), SparkJobErrorType.InvalidConfig)
      }

      // copy defaults and instance options
      (inst.id, inst.copy(parameters = procConfig.parameters ++ inst.parameters), klass)

    }).map(tuple => {
      val proc = tuple._3.getDeclaredConstructor(classOf[Instance]).newInstance(tuple._2)

      (tuple._1, proc)

    }).foldLeft(Map[String, Any]())((map: Map[String, Any], item: (String, Any)) => {
      map ++ Map(item._1 -> item._2)
    })

    // Ends will all items (id -> Either Processor Source), adds those to the instances map.
    val stages: Map[String, Any] = config.sources.map(src => {
      val srcClass: Class[Source] = createClass[Source](src.classname)

      val srcInstance = srcClass.getDeclaredConstructor(classOf[SourceCfg], classOf[StreamingContext]).newInstance(src, ssc)

      (src, srcInstance)
    }).foldLeft(instances)((map: Map[String, Any], item: (SourceCfg, Any)) => {
      map ++ Map(item._1.id -> item._2)
    })

    // connect all the flows together
    config.flows.foreach(flow => {
      val seq = flow.sequence.map(id => {
        stages.get(id) match {
        case Some(xid) => xid
        case _ => throw new SparkJobException("Cannot find a process with id %s, required for flow.".format(id), SparkJobErrorType.InvalidConfig)
        }        
      }).toList

      val sourceStream = seq.head.asInstanceOf[Source].stream()

      logger.info("Source: " + seq.head + " stream " + sourceStream)

      // first should always be a Source object
      seq.tail.foldLeft[DStream[DataItem]](sourceStream)((stream: DStream[DataItem], inst) => {
        logger.info("Next: " + inst + " on " + stream)

        val ret = inst.asInstanceOf[Processor].process(stream)

        logger.info("Returned: " + ret)

        ret
      })
    })
  }

  def createClass[T](classname: String): Class[T] = {
    Class.forName(classname).asInstanceOf[Class[T]]
  }

  def parseJsonFile(config: AppConfig): StreamConfig = {
    val json = try {
      scala.io.Source.fromFile(config.configFile).mkString
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
        throw SparkJobException("Cannot parse the JSON config file: ".format(config.configFile.getName), SparkJobErrorType.InvalidConfig)

      }
    }
  }

}