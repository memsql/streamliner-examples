package com.memsql.spark.examples.kafka

import com.memsql.spark.etl.api.{UserExtractConfig, PhaseConfig, ByteArrayExtractor}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.streaming.kafka.{CheckpointedDirectKafkaInputDStream, CheckpointedKafkaUtils}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * A checkpointing enabled Kafka extractor configured by a Zookeeper quorum.
  * The configuration has 2 required fields:
  *   zk_quorum: a comma delimited string of host1:port,host2:port,host3:port denoting the Zookeeper quorum.
  *   topic: the Kafka topic to extract.
  */
class CheckpointingKafkaExtractor extends ByteArrayExtractor {
  var CHECKPOINT_DATA_VERSION = 1

  var dstream: CheckpointedDirectKafkaInputDStream[String, Array[Byte], StringDecoder, DefaultDecoder, Array[Byte]] = null

  var zkQuorum: String = null
  var topic: String = null

  override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    val kafkaConfig  = config.asInstanceOf[UserExtractConfig]
    zkQuorum = kafkaConfig.getConfigString("zk_quorum").getOrElse {
      throw new IllegalArgumentException("\"zk_quorum\" must be set in the config")
    }
    topic = kafkaConfig.getConfigString("topic").getOrElse {
      throw new IllegalArgumentException("\"topic\" must be set in the config")
    }
  }

  def extract(ssc: StreamingContext, extractConfig: PhaseConfig, batchDuration: Long, logger: PhaseLogger): InputDStream[Array[Byte]] = {
    val kafkaParams = Map[String, String](
      "memsql.zookeeper.connect" -> zkQuorum
    )
    val topics = Set(topic)

    dstream = CheckpointedKafkaUtils.createDirectStreamFromZookeeper[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topics, batchDuration, lastCheckpoint)
    dstream
  }

  override def batchCheckpoint: Option[Map[String, Any]] = {
    dstream match {
      case null => None
      case default => {
        val currentOffsets = dstream.getCurrentOffsets.map { case (tp, offset) =>
          Map("topic" -> tp.topic, "partition" -> tp.partition, "offset" -> offset)
        }
        Some(Map("offsets" -> currentOffsets, "zookeeper" -> zkQuorum, "version" -> CHECKPOINT_DATA_VERSION))
      }
    }
  }

  override def batchRetry: Unit = {
    if (dstream.prevOffsets != null) {
      dstream.setCurrentOffsets(dstream.prevOffsets)
    }
  }
}
