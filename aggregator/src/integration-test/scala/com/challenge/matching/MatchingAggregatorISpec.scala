package com.challenge.matching

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.util.UUID
import com.challenge.matching.config.MatchingAggregatorConfigurationLoader
import com.challenge.matching.config.MatchingAggregatorConfiguration
import com.mongodb.client.model.geojson.{PolygonCoordinates, Position, Polygon => MPolygon}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.typesafe.config.{Config, ConfigFactory}
import com.vividsolutions.jts.geom.Polygon
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.ConfigFactory
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.utils.Time
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Try
import scala.concurrent.duration._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class MatchingAggregatorISpec extends FlatSpec with Matchers with BeforeAndAfterAll with Eventually
  with MatchingAggregatorConfigurationLoader
  with KafkaTools with MongoTools {

  trait ReferentialMock extends Referential{

    override def getPolygon(url: String): List[(String, String)] = {
      Source.fromInputStream(getClass.getResourceAsStream("/polygon.csv")).getLines().toList.map(line => {
        val arr = line.split("\\|")
        (arr(0), arr(1))
      }) //TODO use real HTTP client
    }
  }

  "MatchingAggregator" should "aggregate and save in mongoDB" in {
    val matchingAggregator = new MatchingAggregator() with ReferentialMock
    val ssc = matchingAggregator.apply()
    try {


      ssc.start()
      eventually(timeout(60.seconds), interval(1.seconds)) {

        val request: DataFrame = readData(matchingAggregator, config.requestCollection)
        request.select("polygonName", "rate").collect().toList should contain theSameElementsAs List(Row("NW", 0.5), Row("NE", 0.75))


        val driver: DataFrame = readData(matchingAggregator, config.driverCollection)
        driver.select("polygonName", "rate").collect().toList should contain theSameElementsAs List(Row("NW", 0.75), Row("NE", 0.5))
      }
    } finally {
      Try(ssc.stop())
      Try(matchingAggregator.sparkSession.stop())
    }


  }


  private def readData(matchingAggregator: MatchingAggregator, collection: String) = {
    val readConfig = ReadConfig(Map("uri" -> config.mongoDBUri, "database" -> config.mongoDatabase, "collection" -> collection))
    val actualDf: DataFrame = MongoSpark.builder().readConfig(readConfig).sparkSession(matchingAggregator.sparkSession).build().toDF()
    actualDf
  }

  override def beforeAll(): Unit = {
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,b1b7d6ba-b71b-4a01-a83d-896941bc8a48,2.241223,48.915516,cdcef632-5dbe-49d3-b0c7-28ad274aa7a3,2.241223,48.915516,true,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,1959b6f4-2a0e-4bf3-9f2b-939777ebbaf2,2.241223,48.915516,e6addb30-c489-481b-883b-132388cd7e4b,2.241223,48.915516,false,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,7bc076cf-ec62-4f75-af61-311b4741cca9,2.670597,49.094375,79874ad1-5549-4212-bc80-e666c7674745,2.670597,49.094375,true,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,89757162-e235-44e3-bac7-cf658232a00b,2.670597,49.094375,2599dca2-5515-4525-a2b5-4e17fa162542,2.670597,49.094375,false,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,7bc076cf-ec62-4f75-af61-311b4741cca9,2.670597,49.094375,79874ad1-5549-4212-bc80-e666c7674745,2.113474,48.961241,true,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,89757162-e235-44e3-bac7-cf658232a00b,2.670597,49.094375,2599dca2-5515-4525-a2b5-4e17fa162542,2.113474,48.961241,true,1506880804", config.brokers)
    produce(config.topicName, null, "0dab30aa28caae9e5299051a460cb8ac78224c0d,89757162-e235-44e3-bac7-cf658232a00b,-73.5643,45.506651,2599dca2-5515-4525-a2b5-4e17fa162542,-73.5643,45.506651,false,1506880804", config.brokers)
  }

  override def afterAll(): Unit = {
    Try(zkClient.deleteTopic(config.topicName))
    Try(mongoClient.dropDatabase(config.mongoDatabase))

  }

}