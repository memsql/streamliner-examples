package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api.{UserTransformConfig, SimpleByteArrayTransformer}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import spray.json.JsValue
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

class AvroTransformer extends SimpleByteArrayTransformer {
  def AvroRDDToDataFrame(sqlContext: SQLContext, rdd: RDD[Array[Byte]], schemaJsonString: String): DataFrame = {

    val rowRDD: RDD[Row] = rdd.mapPartitions({ partition =>
      val parser: Schema.Parser = new Schema.Parser()
      val avroSchema = parser.parse(schemaJsonString)
      val reader = new SpecificDatumReader[GenericData.Record](avroSchema)

      partition.map({ bytes =>
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val record = reader.read(null, decoder)
        val avroToRow = new AvroToRow()

        avroToRow.getRow(record)
      })
    })

    val topParser: Schema.Parser = new Schema.Parser()
    var topAvroSchema: Schema = topParser.parse(schemaJsonString)
    sqlContext.createDataFrame(rowRDD, AvroToSchema.getSchema(topAvroSchema))
  }

  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: PhaseLogger): DataFrame = {
    val avroSchemaJson = config.getConfigJsValue("avroSchema") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("Avro schema must be set in the config")
    }
    AvroRDDToDataFrame(sqlContext, rdd, avroSchemaJson.toString)
  }
}


