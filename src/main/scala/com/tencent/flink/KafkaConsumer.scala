package com.tencent.flink

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import com.tencent.flink.MessageOuterClass.Message
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.types.DeserializationException

import scala.util.{Failure, Success, Try}

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
    val pb_consumer = new FlinkKafkaConsumer[Message]("stream_protobuf",
      new ProtobufDeserializationSchema[Message](Message.parseFrom), properties)
    // 方式一：从头开始消费，这种方式会导致重复消费
    // consumer.setStartFromEarliest()
    // 方式二：忽略kafka已有的消息，从最新的位置消费，该方式会导致有些消息没被消费
    // consumer.setStartFromLatest()
    // 方式三：指定时间往后消费,注意：时间不能>=当前时间
    // consumer.setStartFromTimestamp(System.currentTimeMillis()-1000*60*60)
    json_consumer.setCommitOffsetsOnCheckpoints(true)
    pb_consumer.setCommitOffsetsOnCheckpoints(true)

    System.out.println(f"Start consumers, bootstrap.servers='$bootstrap_servers', group.id='$group_id'")

    val left: DataStream[CustomMessage] = env.addSource(json_consumer)
      .flatMap(new JsonStringToFlatMap)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      //.keyBy(_.key)

    val right: DataStream[CustomMessage] = env.addSource(pb_consumer)
      .flatMap(new MessageToFlatMap)
      .assignTimestampsAndWatermarks(new MessageTimeAssigner)
      //.keyBy(_.key)

    left.print()
    right.print()

//    val res: DataStream[String] = left.intervalJoin(right)
//      .between(Time.milliseconds(-5000), Time.milliseconds(5000))
//      .process(new ProcessJoinFunction[RawMessage, RawMessage, String] {
//        override def processElement(left: RawMessage, right: RawMessage,
//                                    ctx: ProcessJoinFunction[RawMessage, RawMessage, String]#Context,
//                                    out: Collector[String]): Unit = {
//          out.collect("Joined> " + left._1 + ": " + left._2 + "-" + right._2)
//        }
//      })
//
//    res.print()

    // execute program
    env.execute("KafkaConsumer")
  }

  /**
   * Protobuf deserialization
   * https://gist.github.com/ankitcha/1283652bb4b858aee0b729d5eb3d47bd
   */
  class ProtobufDeserializationSchema[T](parser: Array[Byte] => T) extends DeserializationSchema[T] {

    override def deserialize(bytes: Array[Byte]): T = {
      Try(parser.apply(bytes)) match {
        case Success(element) =>
          element
        case Failure(e) =>
          throw new DeserializationException(s"Unable to de-serialize bytes", e)
      }
    }

    override def isEndOfStream(nextElement: T): Boolean = false
    override def getProducedType: TypeInformation[T] = null
  }

  case class CustomMessage(key: String, value: String, timestamp: Int)
  implicit val msgTypeInfo = TypeInformation.of(classOf[CustomMessage])

  /**
   * Deserialize Protobuf/JSON(String) to RawMessage
   */
  // PB -> RawMessage
  private class MessageToFlatMap extends FlatMapFunction[Message, CustomMessage] {
    override def flatMap(value: Message, out: Collector[CustomMessage]): Unit = {
      (value.hasKey(), value.hasTimestamp(), value) match {
        case (true, true, elem) => {
          out.collect(CustomMessage(elem.getKey(), elem.getValue(), elem.getTimestamp()))
        }
        case _ =>
      }
    }
  }
  // JSON -> RawMessage
  private class JsonStringToFlatMap extends FlatMapFunction[String, CustomMessage] {
    lazy val jsonParser = new ObjectMapper()
    override def flatMap(value: String, out: Collector[CustomMessage]): Unit = {
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      (jsonNode.has("key"), jsonNode.has("timestamp"), jsonNode) match {
        case (true, true, elem) => {
          out.collect(CustomMessage(elem.get("key").asText(), elem.get("value").asText(), elem.get("timestamp").asInt()))
        }
        case _ =>
      }
    }
  }

  /**
   * Assigns timestamps to RawMessage based on internal timestamp and
   * emits watermarks with five seconds slack.
   */
  class MessageTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[CustomMessage](Time.seconds(5)) {
    override def extractTimestamp(r: CustomMessage): Long = r.timestamp
  }

}
