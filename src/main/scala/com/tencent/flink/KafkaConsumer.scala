package com.tencent.flink

import java.io.IOException
import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import com.tencent.flink.MessageOuterClass.Message


object KafkaConsumer {
  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    // Set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)

    // Init kafka consumer
    val properties = new Properties()
    val bootstrap_servers = parameter.get("bootstrap.servers", "kafka:9092")
    val group_id = parameter.get("group.id", "oceanus-playground")
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id)

    val json_consumer = new FlinkKafkaConsumer[String]("stream_json", new SimpleStringSchema(), properties)
    val pb_consumer = new FlinkKafkaConsumer[Array[Byte]]("stream_protobuf",
      new ByteArrayDeserializationSchema[Array[Byte]](), properties)
    // 方式一：从头开始消费，这种方式会导致重复消费
    // consumer.setStartFromEarliest()
    // 方式二：忽略kafka已有的消息，从最新的位置消费，该方式会导致有些消息没被消费
    // consumer.setStartFromLatest()
    // 方式三：指定时间往后消费,注意：时间不能>=当前时间
    // consumer.setStartFromTimestamp(System.currentTimeMillis()-1000*60*60)
    json_consumer.setCommitOffsetsOnCheckpoints(true)
    pb_consumer.setCommitOffsetsOnCheckpoints(true)

    System.out.println(f"Start consumers, bootstrap.servers='$bootstrap_servers', group.id='$group_id'")

//    val left: DataStream[(String, Double)] = env.addSource(json_consumer)
//      .flatMap(new RandToFlatMap)

    val right: DataStream[Array[Byte]] = env.addSource(pb_consumer)
    right.map{ raw => Message.parseFrom(raw) }
      .print()

//    left.join(right)
//      .where(_.key)
//      .equalTo(_.key)
//      .timeWindow(Time.seconds(5))
//      .print()
//      .apply { (g, s) => Person(g.name, g.grade, s.salary) }

    // execute program
    env.execute("KafkaConsumer")
  }

  /**
   * Deserialize Array[Byte] from kafka (like protobuf)
   */
  class ByteArrayDeserializationSchema[T] extends AbstractDeserializationSchema[Array[Byte]]{
    @throws[IOException]
    override def deserialize(message: Array[Byte]): Array[Byte] = message
  }

  /**
   * Deserialize JSON from kafka
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
