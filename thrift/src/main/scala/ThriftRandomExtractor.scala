package com.memsql.spark.examples.thrift

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.StreamingContext
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.{TBase, TFieldIdEnum, TSerializer}

class ThriftRandomExtractor extends Extractor {
  var count: Int = 1
  var thriftType: Class[_] = null
  var serializer: TSerializer = null

  def schema: StructType = StructType(StructField("bytes", BinaryType, false) :: Nil)

  override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    val userConfig = config.asInstanceOf[UserExtractConfig]
    val className = userConfig.getConfigString("className") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("className must be set in the config")
    }
    thriftType = Class.forName(className)
    serializer = new TSerializer(new TBinaryProtocol.Factory())
    count = userConfig.getConfigInt("count").getOrElse(1)
  }

  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Option[DataFrame] = {
    val rdd = sqlContext.sparkContext.parallelize((1 to count).map(_ => Row({
      val thriftObject = ThriftRandomGenerator.next(thriftType).asInstanceOf[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]
      serializer.serialize(thriftObject)
    })))
    Some(sqlContext.createDataFrame(rdd, schema))
  }
}
