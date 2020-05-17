package com.zzz.flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


object FLinkJoinTest {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    // Set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    // Init kafka consumer
    val properties = new Properties()
    val bootstrap_servers = parameter.get("bootstrap.servers", "kafka:9092")
    val group_id = parameter.get("group.id", "flink-playground")
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id)

    val consumer1 = new FlinkKafkaConsumer[CustomMessage]("stream1", new JSONMessageDeSchema(), properties)
    val consumer2 = new FlinkKafkaConsumer[CustomMessage]("stream2", new JSONMessageDeSchema(), properties)
    // 方式一：从头开始消费，这种方式会导致重复消费
    // consumer.setStartFromEarliest()
    // 方式二：忽略kafka已有的消息，从最新的位置消费，该方式会导致有些消息没被消费
    // consumer.setStartFromLatest()
    // 方式三：指定时间往后消费,注意：时间不能>=当前时间
    // consumer.setStartFromTimestamp(System.currentTimeMillis()-1000*60*60)
    consumer1.setCommitOffsetsOnCheckpoints(true)
    consumer2.setCommitOffsetsOnCheckpoints(true)

    val left = env.addSource(consumer1)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      .name("stream1")
      .keyBy(elem => elem.key)

    val right = env.addSource(consumer2)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      .name("stream2")
      .keyBy(elem => elem.key)

    val res: DataStream[String] = left.intervalJoin(right)
      .between(Time.milliseconds(-30000), Time.milliseconds(3000))
      .process(new CombineJoinFunction)
      .name("intervalJoin")

    // print in STDOUT (docker logs)
    res.print()

    // execute program
    env.execute("KafkaConsumer")
  }

  /**
   * Deserialization CustomMessage from kafka
   */
  case class CustomMessage(key: String, value: String, int: Int, float: Double, timestamp: Long)

  // JSON -> CustomMessage
  private class JSONMessageDeSchema extends DeserializationSchema[CustomMessage] {
    lazy val jsonParser = new ObjectMapper()

    override def deserialize(bytes: Array[Byte]): CustomMessage = {
      val elem = jsonParser.readValue(bytes, classOf[JsonNode])
      val int = if (elem.has("int")) elem.get("int").asInt else 0
      val float = if (elem.has("float")) elem.get("float").asDouble else 0.0

      LOG.debug(f"JSON[${elem.get("timestamp").asLong}] key=${elem.get("key").asText} int=${int} float=${float}")
      CustomMessage(elem.get("key").asText, elem.get("value").asText, int, float, elem.get("timestamp").asLong)
    }

    override def isEndOfStream(nextElement: CustomMessage): Boolean = false
    override def getProducedType: TypeInformation[CustomMessage] = createTypeInformation[CustomMessage]
  }

  /**
   * Assigns timestamps to RawMessage based on internal timestamp and
   * emits watermarks with five seconds slack.
   */
  private class MessageTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[CustomMessage](Time.seconds(5)) {
    override def extractTimestamp(r: CustomMessage): Long = r.timestamp
  }

  private class CombineJoinFunction
    extends ProcessJoinFunction[CustomMessage, CustomMessage, String] {

    override def processElement(left: CustomMessage, right: CustomMessage,
                                ctx: ProcessJoinFunction[CustomMessage, CustomMessage, String]#Context,
                                out: Collector[String]): Unit = {
      val format_string = f"[${left.timestamp}] Join left=${left.key} right=${right.key})," +
                          f" int=${left.int} float=${right.float}"
      LOG.info(format_string)
      out.collect(format_string)
    }
  }
}

