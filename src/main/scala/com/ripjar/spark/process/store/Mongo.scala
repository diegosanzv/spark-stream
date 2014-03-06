package com.ripjar.spark.process.store

import com.ripjar.spark.data._
import org.slf4j.LoggerFactory
import org.apache.spark.streaming.dstream.DStream
import com.mongodb._
import com.mongodb.util.JSON
import com.ripjar.spark.process.{TerminalProcessor, Processor}
import scala.Some
import com.ripjar.spark.job.InstanceConfig
import org.json4s.DefaultFormats
import java.util
import scala.collection.mutable

/*
 * Used to store a stream in Mongo
 * 
 * Config parameters: 
 *  	brokers
 *   	route
 * 
 * Task parameters:
 * 		"route": "route" 
 * 
 * The tasks route overloads the default route
 */
object Mongo {
  val logger = LoggerFactory.getLogger(classOf[Mongo])

  val collections: mutable.Map[String, DBCollection] = mutable.Map.empty

  def getDBCollection(config: MongoConfig, data: MongoData): DBCollection = {
    collections.synchronized {
      collections.get(config.hash) match {
        case Some(c) => c
        case None => {
          val mo = new MongoOptions()
          mo.connectionsPerHost = config.connectionsPerHost
          mo.threadsAllowedToBlockForConnectionMultiplier = config.threadsAllowedToBlockForConnectionMultiplier

          val mongoDB = new com.mongodb.Mongo(config.mongoHost, mo).getDB(config.mongoDbName)
          val collection = mongoDB.getCollection(config.mongoCollectionName)
          val concern = WriteConcern.NORMAL

          concern.continueOnErrorForInsert(true)
          collection.setWriteConcern(concern)

          collections += ((config.hash, collection))

          // create the indices
          data.indices.foreach(index => {
            val obj = new BasicDBObject()

            index.keys.foreach(key => {
              obj.put(key, 1)
            })

            println("creating index: " + index)

            collection.ensureIndex(obj, index.name)
          })

          collection
        }
      }
    }
  }
}

class MongoConfig(val mongoHost: String, val mongoDbName: String, val mongoCollectionName: String, val connectionsPerHost: Int, val threadsAllowedToBlockForConnectionMultiplier: Int) extends Serializable {
  val hash = mongoHost + ":" + mongoDbName + ":" + mongoCollectionName + ":" + connectionsPerHost + ":" + threadsAllowedToBlockForConnectionMultiplier
}

case class MongoIndices(name: String, keys: List[String])
case class MongoData(indices: List[MongoIndices])

class Mongo(config: InstanceConfig) extends TerminalProcessor with Serializable {
  val mongoConfig = new MongoConfig(
    config.getMandatoryParameter("host"),
    config.getMandatoryParameter("db"),
    config.getMandatoryParameter("collection"),
    10,
    100)

  val mongoData = {
    implicit val formats = DefaultFormats

    config.data.extract[MongoData]
  }

  override def process(stream: DStream[DataItem]): DStream[DataItem] = {
    stream.foreachRDD( rdd => {
      rdd.foreachPartition(iter => {
        val coll = Mongo.getDBCollection(mongoConfig, mongoData)

        iter.foreach( item => {
          val dbObject = JSON.parse(item.toString).asInstanceOf[DBObject]

          coll.save(dbObject)
        })
      })
    })

    stream
  }
}