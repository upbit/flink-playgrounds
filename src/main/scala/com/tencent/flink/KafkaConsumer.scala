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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows


object KafkaConsumer {
  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    // Set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

    val timeExtractor: BoundedOutOfOrdernessTimestampExtractor[(String, String, Int)] =
      new BoundedOutOfOrdernessTimestampExtractor[(String, String, Int)](Time.milliseconds(10000)) {
      override def extractTimestamp(element: (String, String, Int)): Long = element._3
    }

    val left: DataStream[(String, String, Int)] = env.addSource(json_consumer)
      .flatMap(new JsonToFlatMap)
      .assignTimestampsAndWatermarks(timeExtractor)

    val right: DataStream[(String, String, Int)] = env.addSource(pb_consumer)
      .flatMap(new ProtoMessageToFlatMap)
      .assignTimestampsAndWatermarks(timeExtractor)

    val res: DataStream[String] = left.keyBy(_._1).intervalJoin(right.keyBy(_._1))
      .between(Time.milliseconds(-60000), Time.milliseconds(60000))
      .process(new ProcessJoinFunction[(String, String, Int), (String, String, Int), String] {
        override def processElement(left: (String, String, Int), right: (String, String, Int),
                                    ctx: ProcessJoinFunction[(String, String, Int), (String, String, Int),
                                      String]#Context, out: Collector[String]): Unit = {
          out.collect("Joined> " + left._1 + ": " + left._2 + "-" + right._2)
        }
      })

    res.print()

    // execute program
    env.execute("KafkaConsumer")
  }

  /**
   * Deserialize Array[Byte] from kafka (like protobuf)
   */
  private class ByteArrayDeserializationSchema[T] extends AbstractDeserializationSchema[Array[Byte]]{
    @throws[IOException]
    override def deserialize(message: Array[Byte]): Array[Byte] = message
  }

  private class ProtoMessageToFlatMap extends FlatMapFunction[Array[Byte], (String, String, Int)] {

    override def flatMap(value: Array[Byte], out: Collector[(String, String, Int)]): Unit = {
      // deserialize Protobuf
      val msg = Message.parseFrom(value)

      (msg.hasKey(), msg.hasTimestamp(), msg) match {
        case (true, true, elem) => {
          out.collect((elem.getKey(), elem.getValue(), elem.getTimestamp()))
        }
        case _ =>
      }
    }
  }

  /**
   * Deserialize JSON from kafka
   */
  private class JsonToFlatMap extends FlatMapFunction[String, (String, String, Int)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, String, Int)]): Unit = {
      // deserialize JSON
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val hasTimestamp = jsonNode.has("timestamp")

      (hasTimestamp, jsonNode) match {
        case (true, node) => {
          out.collect((
            node.get("key").asText(),
            node.get("value").asText(),
            node.get("timestamp").asInt()
          ))
        }
        case _ =>
      }
    }
  }
}
