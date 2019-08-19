
package com.challenge.matching

import com.challenge.matching.config.MatchingAggregatorConfigurationLoader
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

trait SparkMatchingAggregator {
  self: MatchingAggregatorConfigurationLoader =>

  @transient val sparkSession = SparkSession
    .builder()
    .master(config.sparkMaster)
    .appName(config.appName)
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .getOrCreate()

  GeoSparkSQLRegistrator.registerAll(sparkSession)

  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(config.batchWindow))

}