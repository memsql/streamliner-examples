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
import com.memsql.spark.etl.utils.{JSONPath, JSONUtils}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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

// A Transformer that extracts some fields from a JSON object
// This saves into MemSQL 2 columns of type TEXT
class JSONMultiColsTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {

    // define a JSON path
    val paths = Array[JSONPath](
      JSONPath("id"),
      JSONPath("txt"))

    // return a DataFrame with schema based on the JSON path
    JSONUtils.JSONRDDToDataFrame(paths, sqlContext, rdd.map(r => byteUtils.bytesToUTF8String(r)))
  }
}

// A Transformer that parses a JSON object (using jackson) and filters only objects containing an "id" field
// This saves into MemSQL a single column of type JSON
class JSONCheckIdTransformer extends SimpleByteArrayTransformer {
  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    var columnName = config.getConfigString("table", "column_name").getOrElse("data")

    // transform the RDD into RDD[Row], filtering only objects that contain an "id" field
    val jsonRDD = rdd.map(r => new JsonValue(byteUtils.bytesToUTF8String(r)))
    val filteredRDD = jsonRDD.mapPartitions(r => {
        // register jackson mapper (this needs to be executed on each partition)
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        // filter the partition for only the objects that contain an "id" field
        r.filter(x => mapper.readValue(x.value, classOf[Map[String,Any]]).contains("id"))
    })
    val transformedRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable JSON column using the configured column name
    val schema = StructType(Array(StructField(columnName, JsonType, true)))

    sqlContext.createDataFrame(transformedRDD, schema)
  }
}
