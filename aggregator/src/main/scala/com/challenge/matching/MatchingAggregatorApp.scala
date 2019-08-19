package com.challenge.matching

import java.util.UUID

import com.challenge.matching.config.MatchingAggregatorConfigurationLoader
import com.mongodb.client.model.geojson.{PolygonCoordinates, Position, Polygon => MPolygon}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.vividsolutions.jts.geom.Polygon
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

object MatchingAggregatorApp extends App {

  val matchingAggregator = new MatchingAggregator() with ReferentialHTTP

  val ssc = matchingAggregator.apply()
  ssc.start()
  ssc.awaitTermination()
  ssc.stop()

}

trait Referential {
  def getPolygon(url: String): List[(String, String)]
}

trait ReferentialHTTP extends Referential{
  override def getPolygon(url: String): List[(String, String)] = {
    Source.fromURL(url).getLines().toList.map(line => {
      val arr = line.split("\\|")
      (arr(0), arr(1))
    }) //TODO use real HTTP client
  }
}



class MatchingAggregator extends MatchingAggregatorConfigurationLoader with SparkMatchingAggregator {
  self: Referential=>
  val logger = LoggerFactory.getLogger(getClass)

  import MatchingAggregator._
  import sparkSession.sqlContext.implicits._

  def apply(): StreamingContext = {
    logger.info("apply.....")
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(config.batchWindow))
    apply(stream(ssc))
    ssc
  }

  def polygonDF(sparkSession: SparkSession, url: String) = {
    import sparkSession.sqlContext.implicits._
    implicit val polygonEncoder: Encoder[Polygon] = Encoders.kryo[Polygon]
    sparkSession
      .sparkContext
      .parallelize(getPolygon(url))
      .toDF(List("polygon", "polygonName"): _*)
      .withColumn("polygon", expr("ST_GeomFromWKT(polygon)"))
  }

  private def stream(ssc: StreamingContext) = {
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> config.brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> (config.appName + UUID.randomUUID().toString),
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(config.topicName), kafkaParams)
    )
  }

  def apply(stream: InputDStream[ConsumerRecord[String, String]]) = {
    logger.info("apply with StreamingContext")
    val polygon = polygonDF(sparkSession, config.referentialServiceUrl)


    stream.foreachRDD(rdd => {
      val df = rdd
        .map(line => MatchingDataLine(line.value()))
        .toDF()
        .withColumn(RequestLatitude, col(RequestLatitude).cast(DecimalType(38, 18)))
        .withColumn(RequestLongitude, col(RequestLongitude).cast(DecimalType(38, 18)))
        .withColumn(DriverLatitude, col(DriverLatitude).cast(DecimalType(38, 18)))
        .withColumn(DriverLongitude, col(DriverLongitude).cast(DecimalType(38, 18)))
        .withColumn(RequestPoint, expr(s"ST_Point($RequestLongitude, $RequestLatitude)"))
        .withColumn(DriverPoint, expr(s"ST_Point($DriverLongitude, $DriverLatitude)"))
        .withColumn(IsMatch, col(IsMatch).cast(IntegerType))

      df.persist()

      aggregateAndSave(df, polygon, RequestPoint, config.requestCollection)
      aggregateAndSave(df, polygon, DriverPoint, config.driverCollection)


    }

    )
  }

  def aggregateAndSave(df: DataFrame, polygon: DataFrame, kindPoint: String, collection: String) = {
    val rate =
      df
        .join(polygon,
          expr(
            s"ST_Within($kindPoint, $PolygonColumn)"
          )
        )
        .select(PolygonName, ComputeAt, IsMatch, PolygonColumn)
        .groupBy(PolygonName, ComputeAt)
        .agg(avg(IsMatch) as IsMatch, first(PolygonColumn) as PolygonColumn)
        .rdd //udf must return a known type . Use rdd to avoid :  java.lang.UnsupportedOperationException: Schema for type org.bson.Document is not supported
        .map(row => buildDocument(row))
    MongoSpark.save(rate, WriteConfig(config.mongoDatabase, collection, Some(config.mongoDBUri)))
  }
}


object MatchingAggregator {
  val RequestLatitude = "requestLatitude"
  val RequestLongitude = "RequestLongitude"
  val DriverLatitude = "DriverLatitude"
  val DriverLongitude = "driverLongitude"
  val RequestPoint = "requestPoint"
  val DriverPoint = "driverPoint"
  val IsMatch = "isMatch"
  val PolygonColumn = "polygon"
  val PolygonName = "polygonName"
  val ComputeAt = "computeAt"
  val Rate = "rate"

  val buildDocument: Row => Document = { row =>
    val polygonName = row.getString(0)
    val computeAt = row.getLong(1)
    val rate = row.getDouble(2)
    val regionPolygon = row.get(3).asInstanceOf[Polygon]
    val coordinates = new PolygonCoordinates(regionPolygon.getCoordinates.toList.map(c => new Position(c.x, c.y)).asJava)
    val polygon = new MPolygon(coordinates)
    new Document(PolygonName, polygonName)
      .append(ComputeAt, computeAt)
      .append(Rate, rate)
      .append(PolygonColumn, polygon)
  }

}


