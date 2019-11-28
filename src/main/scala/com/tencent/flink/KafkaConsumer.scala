package com.tencent.flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import com.tencent.flink.MessageOuterClass.Message
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector


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

    val json_consumer = new FlinkKafkaConsumer[CustomMessage]("stream_json", new JSONMessageDeSchema(), properties)
    val pb_consumer = new FlinkKafkaConsumer[CustomMessage]("stream_protobuf",
      new ProtobufMessageDeSchema(), properties)
    // 方式一：从头开始消费，这种方式会导致重复消费
    // consumer.setStartFromEarliest()
    // 方式二：忽略kafka已有的消息，从最新的位置消费，该方式会导致有些消息没被消费
    // consumer.setStartFromLatest()
    // 方式三：指定时间往后消费,注意：时间不能>=当前时间
    // consumer.setStartFromTimestamp(System.currentTimeMillis()-1000*60*60)
    json_consumer.setCommitOffsetsOnCheckpoints(true)
    pb_consumer.setCommitOffsetsOnCheckpoints(true)

    System.out.println(f"Start consumers, bootstrap.servers='$bootstrap_servers', group.id='$group_id'")

    val left = env.addSource(json_consumer)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      .keyBy(_.key)

    val right = env.addSource(pb_consumer)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      .keyBy(_.key)

//    left.print()
//    right.print()

    val res: DataStream[String] = left.intervalJoin(right)
      .between(Time.milliseconds(-1000), Time.milliseconds(1000))
      .process(new ProcessJoinFunction[CustomMessage, CustomMessage, String] {
        override def processElement(left: CustomMessage, right: CustomMessage,
                                    ctx: ProcessJoinFunction[CustomMessage, CustomMessage, String]#Context,
                                    out: Collector[String]): Unit = {
          out.collect("Joined> " + left.key + ": " + left.value + "-" + right.value)
        }
      })

    res.print()

    // execute program
    env.execute("KafkaConsumer")
  }

  case class CustomMessage(key: String, value: String, timestamp: Int)

  // Protobuf -> CustomMessage
  private class ProtobufMessageDeSchema extends DeserializationSchema[CustomMessage] {

    override def deserialize(bytes: Array[Byte]): CustomMessage = {
      val elem = Message.parseFrom(bytes)
      CustomMessage(elem.getKey(), elem.getValue(), elem.getTimestamp())
    }

    override def isEndOfStream(nextElement: CustomMessage): Boolean = false
    override def getProducedType: TypeInformation[CustomMessage] = createTypeInformation[CustomMessage]
  }
  // JSON -> CustomMessage
  private class JSONMessageDeSchema extends DeserializationSchema[CustomMessage] {
    lazy val jsonParser = new ObjectMapper()

    override def deserialize(bytes: Array[Byte]): CustomMessage = {
      val elem = jsonParser.readValue(bytes, classOf[JsonNode])
      CustomMessage(elem.get("key").asText(), elem.get("value").asText(), elem.get("timestamp").asInt())
    }

    override def isEndOfStream(nextElement: CustomMessage): Boolean = false
    override def getProducedType: TypeInformation[CustomMessage] = createTypeInformation[CustomMessage]
  }

  /**
   * Assigns timestamps to RawMessage based on internal timestamp and
   * emits watermarks with five seconds slack.
   */
  class MessageTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[CustomMessage](Time.seconds(5)) {
    override def extractTimestamp(r: CustomMessage): Long = r.timestamp * 1000
  }

}
