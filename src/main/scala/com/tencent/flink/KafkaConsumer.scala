package com.tencent.flink

import java.util.Properties

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object KafkaConsumer {
  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    // topic
    val topic: String = try {
      parameter.getRequired("topic")
    } catch {
      case e: Exception => {
        System.err.println("No topic specified. Please run 'KafkaConsumer --topic <topic>'")
        return
      }
    }

    // set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)

    val properties = new Properties()
    val bootstrap_servers = parameter.get("bootstrap.servers", "kafka:9092")
    val group_id = parameter.get("group.id", "oceanus-playground")

    properties.setProperty("bootstrap.servers", bootstrap_servers)
    properties.setProperty("group.id", group_id)

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    // 方式一：从头开始消费，这种方式会导致重复消费
    // consumer.setStartFromEarliest()
    // 方式二：忽略kafka已有的消息，从最新的位置消费，该方式会导致有些消息没被消费
    // consumer.setStartFromLatest()
    // 方式三：指定时间往后消费,注意：时间不能>=当前时间
    // consumer.setStartFromTimestamp(System.currentTimeMillis()-1000*60*60)
    consumer.setCommitOffsetsOnCheckpoints(true)

    System.out.println(f"Start consumer on $topic, bootstrap.servers='$bootstrap_servers', group.id='$group_id'")

    val stream: DataStream[String] = env.addSource(consumer)
    stream.flatMap(new RandToFlatMap)
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()

    // execute program
    env.execute("KafkaConsumer")
  }

  /**
   * Deserialize JSON from kafka
   *
   * Implements a string tokenizer that splits sentences into words as a
   * user-defined FlatMapFunction.
   */
  private class RandToFlatMap extends FlatMapFunction[String, (String, Double)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Double)]): Unit = {
      // deserialize JSON
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val hasValue = jsonNode.has("value")

      (hasValue, jsonNode) match {
        case (true, node) => {
          val flatKey = node.get("key").asText()
          val flatValue = node.get("value").asDouble()
          out.collect((flatKey, flatValue))
        }
        case _ =>
      }
    }
  }
}
