package com.memsql.streamliner.examples

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.memsql.spark.connector._
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

// The simplest implementation of an Extractor just provides a next method.
// This is useful for prototyping and debugging.
class ConstantExtractor extends Extractor {
  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
   logger: PhaseLogger): Option[DataFrame] = {
    logger.info("extracting a constant sequence DataFrame")

    val schema = StructType(StructField("number", IntegerType, false) :: Nil)

    val sampleData = List(1,2,3,4,5)
    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }
}

// An Extractor can also be configured with the config blob that is provided in
// MemSQL Ops.
class ConfigurableConstantExtractor extends Extractor {
  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long,
   logger: PhaseLogger): Option[DataFrame] = {
    val userConfig = config.asInstanceOf[UserExtractConfig]
    val start = userConfig.getConfigInt("start").getOrElse(1)
    val end = userConfig.getConfigInt("end").getOrElse(5)
    val columnName = userConfig.getConfigString("column_name").getOrElse("number")

    logger.info("extracting a sequence DataFrame from $start to $end")

    val schema = StructType(StructField(columnName, IntegerType, false) :: Nil)

    val sampleData = List.range(start, end + 1)
    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }

}

// A more complex Extractor which maintains some state can be implemented using
// the initialize and cleanup methods.
class SequenceExtractor extends Extractor {
  var i: Int = Int.MinValue

  override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    val userConfig = config.asInstanceOf[UserExtractConfig]
    i = userConfig.getConfigInt("sequence", "initial_value").getOrElse(0)

    logger.info(s"initializing the sequence at $i")
  }

  override def cleanup(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    logger.info("cleaning up the sequence")    
  }

  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Option[DataFrame] = {
    val userConfig = config.asInstanceOf[UserExtractConfig]
    val sequenceSize = userConfig.getConfigInt("sequence", "size").getOrElse(5)

    logger.info(s"emitting a sequence RDD from $i to ${i + sequenceSize}")

    val schema = StructType(StructField("number", IntegerType, false) :: Nil)

    i += sequenceSize
    val sampleData = List.range(i - sequenceSize, i)
    val rowRDD = sqlContext.sparkContext.parallelize(sampleData).map(Row(_))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    Some(df)
  }  
}
