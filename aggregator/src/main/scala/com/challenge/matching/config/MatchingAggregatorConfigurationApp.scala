package com.challenge.matching.config

import com.typesafe.config.{Config, ConfigFactory}

trait MatchingAggregatorConfigurationLoader {
  lazy val config = MatchingAggregatorConfiguration(ConfigFactory.load())
}

case class MatchingAggregatorConfiguration(
                                            sparkMaster: String,
                                            appName: String,
                                            batchWindow: Long,
                                            brokers: String,
                                            topicName: String,
                                            mongoDBUri: String,
                                            mongoDatabase: String,
                                            requestCollection: String,
                                            driverCollection: String,
                                            referentialServiceUrl: String
                                          )

object MatchingAggregatorConfiguration {
  def apply(config: Config): MatchingAggregatorConfiguration = {
    MatchingAggregatorConfiguration(
      config.getString("spark.master"),
      config.getString("spark.appName"),
      config.getInt("spark.batchWindow"),
      config.getString("kafka.brokers"),
      config.getString("kafka.topicName"),
      config.getString("mongodb.uri"),
      config.getString("mongodb.database"),
      config.getString("mongodb.requestCollection"),
      config.getString("mongodb.driverCollection"),
      config.getString("referential.uri")
    )
  }
}