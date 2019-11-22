package com.tencent.oceanus.playgrounds

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaConsumer {
  def main(args: Array[String]) {
    val parameter = ParameterTool.fromArgs(args)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val properties = new Properties()
    val bootstrap_servers = parameter.get("bootstrap_servers", "zookeeper:2181")
    val group_id = parameter.get("group_id", "oceanus-playground")
    val topic = parameter.get("topic", "topic")

    properties.setProperty("bootstrap.servers", bootstrap_servers)
    properties.setProperty("group.id", group_id)

    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
    kafkaSource.setStartFromLatest()
    kafkaSource.setCommitOffsetsOnCheckpoints(true)
    val stream = env.addSource(kafkaSource)

    stream.flatMap{ _.toLowerCase().split("\\W+")filter { _.nonEmpty} }
      .map{(_,1)}
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
