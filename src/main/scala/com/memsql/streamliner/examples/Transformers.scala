package com.memsql.streamliner.examples

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.memsql.spark.context._
import com.memsql.spark.connector._
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger

// A Transformer implements the transform method which turns an RDD into a DataFrame.
class EvenNumbersOnlyTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    logger.info("transforming the RDD")

    // transform the RDD into RDD[Row]
    val integerRDD = rdd.map(byteUtils.bytesToInt)
    val filteredRDD = integerRDD.filter(x => x % 2 == 0)
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable integer column named number
    val schema = StructType(Array(StructField("number", IntegerType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}

// A Transformer can also be configured with the config blob that is provided in MemSQL Ops.
class ConfigurableNumberParityTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    val keepEvenNumbers = config.getConfigBoolean("filter", "even").getOrElse(true)
    val keepOddNumbers = config.getConfigBoolean("filter", "odd").getOrElse(true)
    val columnName = config.getConfigString("table", "column_name").getOrElse("number")

    logger.info("transforming the RDD")

    // transform the RDD into RDD[Row] using the configured filter
    val integerRDD = rdd.map(byteUtils.bytesToInt)
    val filteredRDD = integerRDD.filter(x => (x % 2 == 0 && keepEvenNumbers) || (x % 2 == 1 && keepOddNumbers))
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable integer column using the configured column name
    val schema = StructType(Array(StructField(columnName, IntegerType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}

class JSONCheckIdTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    var columnName = config.getConfigString("table", "column_name").getOrElse("data")

    // transform the RDD into RDD[Row], filtering only objects that contain an "id" field
    val jsonRDD = rdd.map(r => new JsonValue(byteUtils.bytesToUTF8String(r)))
    val filteredRDD = jsonRDD.filter(x => JSON.parseFull(x.value).get.asInstanceOf[Map[String, Any]].contains("id"))
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable JSON column using the configured column name
    val schema = StructType(Array(StructField(columnName, JsonType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
