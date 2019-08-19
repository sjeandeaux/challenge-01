package com.challenge.matching


import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Time

trait KafkaTools {


  val kafkaZkClient = KafkaZkClient("zookeeper:2181",
    false,
    10000,
    8000,
    Integer.MAX_VALUE,
    Time.SYSTEM
  )

  val zkClient = new AdminZkClient(kafkaZkClient)

  def produce[T <: AnyRef](topic: String, key: String, message: String, brokers: String): Unit = {
    val configuration = {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props
    }

    val producer = new KafkaProducer[String, String](configuration)
    val data = new ProducerRecord[String, String](topic, key, message)

    producer.send(data).get(60, TimeUnit.SECONDS)
    producer.close()
  }


}
