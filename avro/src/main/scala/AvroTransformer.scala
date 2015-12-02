package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api.{UserTransformConfig, Transformer, PhaseConfig}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.StructType

import spray.json.JsValue
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

class AvroTransformer extends Transformer {
  val parser: Schema.Parser = new Schema.Parser()
  var reader: SpecificDatumReader[GenericData.Record] = null
  var avroSchema: Schema = null
  var schema: StructType = null  

  def AvroRDDToDataFrame(sqlContext: SQLContext, rdd: RDD[Row]): DataFrame = {
    val rowRDD: RDD[Row] = rdd.mapPartitions({ partition =>
      partition.map({ rowOfBytes =>
        val bytes = rowOfBytes(0).asInstanceOf[Array[Byte]]
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val record = reader.read(null, decoder)
        val avroToRow = new AvroToRow()

        avroToRow.getRow(record)
      })
    })
    sqlContext.createDataFrame(rowRDD, schema)
  }

  override def initialize(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    val avroSchemaJson = userConfig.getConfigJsValue("avroSchema") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("avroSchema must be set in the config")
    }

    avroSchema = parser.parse(avroSchemaJson.toString)
    schema = AvroToSchema.getSchema(avroSchema)

    reader = new SpecificDatumReader[GenericData.Record](avroSchema)
  }

  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    AvroRDDToDataFrame(sqlContext, df.rdd)
  }
}


