package com.memsql.streamliner.examples

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.api.configs._

class ConstantExtractor extends SimpleByteArrayExtractor {
  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: Logger): Option[RDD[Array[Byte]]] = {
    logger.log(Level.INFO, "emitting a constant RDD")

    Some(sparkContext.parallelize(List(1,2,3,4,5).map(byteUtils.intToBytes)))
  }
}
