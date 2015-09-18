package com.memsql.spark.examples.thrift

import com.memsql.spark.etl.api.SimpleByteArrayExtractor
import com.memsql.spark.etl.api.configs.UserExtractConfig
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.{TBase, TFieldIdEnum, TSerializer}

class ThriftRandomExtractor extends SimpleByteArrayExtractor {
  var count: Int = 1
  var thriftType: Class[_] = null
  var serializer: TSerializer = null

  override def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: Logger): Unit = {
    val className = config.getConfigString("className") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("className must be set in the config")
    }
    thriftType = Class.forName(className)
    serializer = new TSerializer(new TBinaryProtocol.Factory())
    count = config.getConfigInt("count").getOrElse(1)
  }

  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: Logger): Option[RDD[Array[Byte]]] = {
    val rdd = sparkContext.parallelize((1 to count).map(_ => {
      val thriftObject = ThriftRandomGenerator.next(thriftType).asInstanceOf[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]
      serializer.serialize(thriftObject)
    }))
    Some(rdd)
  }
}
