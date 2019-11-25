package com.tencent.flink

import java.util.Properties

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


// java -classpath /opt/KafkaGenerator.jar:/opt/flink/lib/* com.tencent.flink.KafkaGenerator --bootstrap.servers kafka:9092
object KafkaGenerator {
  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    val properties = new Properties()
    val topic = parameter.get("topic", "stream_json")
    val bootstrap_servers = parameter.get("bootstrap.servers", "kafka:9092")
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    System.out.println(f"Generator data to: $topic")

    val producer = new KafkaProducer[String, String](properties)
    while (true) {
      val record = new ProducerRecord[String, String](topic, "foo", "{\"foo\": \"bar\"}")
      producer.send(record)
      Thread.sleep(1000)

      System.out.print(".")
    }

    producer.close()
  }
}
