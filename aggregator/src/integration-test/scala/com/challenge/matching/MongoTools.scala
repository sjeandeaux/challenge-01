package com.challenge.matching

import com.challenge.matching.config.MatchingAggregatorConfigurationLoader
import com.mongodb.MongoClient

trait MongoTools {
  self: MatchingAggregatorConfigurationLoader =>

  val mongoClient = new MongoClient(new com.mongodb.MongoClientURI(config.mongoDBUri))

}