package com.memsql.streamliner.examples

import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.memsql.spark.connector.dataframe.{JsonType, JsonValue}
import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.{ByteUtils, PhaseLogger}
import com.memsql.spark.etl.utils.{JSONPath, JSONUtils}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConversions._

// A helper object to extract the first column of a schema
object ExtractFirstStructField {
  def unapply(schema: StructType): Option[(String, DataType, Boolean, Metadata)] = schema.fields match {
    case Array(first: StructField, _*) => Some((first.name, first.dataType, first.nullable, first.metadata))
  }
}

// A Transformer implements the transform method which allows inspecting and transforming the DataFrame.
class EvenNumbersOnlyTransformer extends Transformer {
  def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    logger.info("transforming the DataFrame")

    // check that the first column is of type IntegerType and return its name
    val column = df.schema match {
      case ExtractFirstStructField(name: String, dataType: IntegerType, _, _) => name
      case _ => throw new IllegalArgumentException("The first column of the input DataFrame should be IntegerType")
    }

    // filter the dataframe, returning only even numbers
    df.filter(s"$column % 2 = 0")
  }
}

// A Transformer can also be configured with the config blob that is provided in MemSQL Ops.
class ConfigurableNumberParityTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    val keepEvenNumbers = userConfig.getConfigBoolean("filter", "even").getOrElse(true)
    val keepOddNumbers = userConfig.getConfigBoolean("filter", "odd").getOrElse(true)

    // check that the first column is of type IntegerType and return its name
    val column = df.schema match {
      case ExtractFirstStructField(name: String, dataType: IntegerType, _, _) => name
      case _ => throw new IllegalArgumentException("The first column of the input DataFrame should be IntegerType")
    }

    // filter the dataframe, returning resp. nothing, odd, even or all numbers
    logger.info(s"transforming the DataFrame: $keepEvenNumbers, $keepOddNumbers")
    if (!keepEvenNumbers && !keepOddNumbers) {
      df.filter("1 = 0")
    }
    else if (!keepEvenNumbers) {
      df.filter(s"$column % 2 = 1")
    }
    else if (!keepOddNumbers) {
      df.filter(s"$column % 2 = 0")
    } else {
      df
    }
  }
}

// A Transformer that extracts some fields from a JSON object
// It supports both RDD[String] or RDD[Array[Byte]]
// This saves into MemSQL 2 columns of type TEXT
class JSONMultiColsTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    // define a JSON path
    val paths = Array[JSONPath](
      JSONPath("id"),
      JSONPath("txt"))

    // check that the first column is of type StringType or BinaryType and convert the RDD accordingly
    val rdd = df.schema match {
      case ExtractFirstStructField(_, dataType: StringType, _, _) => df.map(r => r(0).asInstanceOf[String])
      case ExtractFirstStructField(_, dataType: BinaryType, _, _) => df.map(r => ByteUtils.bytesToUTF8String(r(0).asInstanceOf[Array[Byte]]))
      case _ => throw new IllegalArgumentException("The first column of the input DataFrame should be either StringType or BinaryType")
    }

    // return a DataFrame with schema based on the JSON path
    JSONUtils.JSONRDDToDataFrame(paths, sqlContext, rdd)
  }  
}

// A Transformer that parses a JSON object (using jackson) and filters only objects containing an "id" field
// This saves into MemSQL a single column of type JSON
class JSONCheckIdTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    val columnName = userConfig.getConfigString("column_name").getOrElse("data")

    // check that the first column is of type StringType or BinaryType and convert the RDD accordingly
    val rdd = df.schema match {
      case ExtractFirstStructField(_, dataType: StringType, _, _) => df.map(r => r(0).asInstanceOf[String])
      case ExtractFirstStructField(_, dataType: BinaryType, _, _) => df.map(r => ByteUtils.bytesToUTF8String(r(0).asInstanceOf[Array[Byte]]))
      case _ => throw new IllegalArgumentException("The first column of the input DataFrame should be either StringType or BinaryType")
    }

    // convert each input element to a JsonValue
    val jsonRDD = rdd.map(r => new JsonValue(r))

    // filters only objects that contain an "id" field
    val filteredRDD = jsonRDD.mapPartitions(r => {
        // register jackson mapper (this needs to be executed on each partition)
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        // filter the partition for only the objects that contain an "id" field
        r.filter(x => mapper.readValue(x.value, classOf[Map[String,Any]]).contains("id"))
    })
    val rowRDD = filteredRDD.map(x => Row(x))

    // create a schema with a single non-nullable JSON column using the configured column name
    val schema = StructType(Array(StructField(columnName, JsonType, true)))

    sqlContext.createDataFrame(rowRDD, schema)
  }
}

class TwitterHashtagTransformer extends Transformer {
  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    val columnName = userConfig.getConfigString("column_name").getOrElse("hashtags")

    // check that the first column is of type StringType or BinaryType and convert the RDD accordingly
    val rdd = df.schema match {
      case ExtractFirstStructField(_, dataType: StringType, _, _) => df.map(r => r(0).asInstanceOf[String])
      case ExtractFirstStructField(_, dataType: BinaryType, _, _) => df.map(r => ByteUtils.bytesToUTF8String(r(0).asInstanceOf[Array[Byte]]))
      case _ => throw new IllegalArgumentException("The first column of the input DataFrame should be either StringType or BinaryType")
    }

    // convert each input element to a JsonValue
    val jsonRDD = df.map(r => r(0).asInstanceOf[String])

    val hashtagsRDD: RDD[String] = jsonRDD.mapPartitions(r => {
      // register jackson mapper (this needs to be instantiated per partition
      // since it is not serializable)
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      r.flatMap(tweet => {
        val rootNode = mapper.readTree(tweet)
        val hashtags = rootNode.path("entities").path("hashtags")
        if (!hashtags.isMissingNode) {
          hashtags.elements
            .filter(n => n.has("text"))
            .map(n => n.get("text").asText)
        } else {
          Nil
        }
      })
    })

    val rowRDD: RDD[Row] = hashtagsRDD.map(x => Row(x))
    val schema = StructType(Array(StructField(columnName, StringType, true)))
    sqlContext.createDataFrame(rowRDD, schema)
  }
}
