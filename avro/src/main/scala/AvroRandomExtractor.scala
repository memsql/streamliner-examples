package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

import java.io.ByteArrayOutputStream

// Generates an RDD of byte arrays, where each is a serialized Avro record.
class AvroRandomExtractor extends Extractor {
  var count: Int = 1
  var generator: AvroRandomGenerator = null
  var writer: DatumWriter[GenericData.Record] = null
  var avroSchema: Schema = null
  
  def schema: StructType = StructType(StructField("bytes", BinaryType, false) :: Nil)

  val parser: Schema.Parser = new Schema.Parser()

  override def initialize(ssc: StreamingContext, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    val userConfig = config.asInstanceOf[UserExtractConfig]
    val avroSchemaJson = userConfig.getConfigJsValue("avroSchema") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("avroSchema must be set in the config")
    }
    count = userConfig.getConfigInt("count").getOrElse(1)
    avroSchema = parser.parse(avroSchemaJson.toString)

    writer = new SpecificDatumWriter(avroSchema)
    generator = new AvroRandomGenerator(avroSchema)
  }

  override def next(ssc: StreamingContext, time: Long, sqlContext: SQLContext, config: PhaseConfig, batchInterval: Long, logger: PhaseLogger): Option[DataFrame] = {
    val rdd = sqlContext.sparkContext.parallelize((1 to count).map(_ => Row({
      val out = new ByteArrayOutputStream
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val avroRecord: GenericData.Record = generator.next().asInstanceOf[GenericData.Record]

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    })))

    Some(sqlContext.createDataFrame(rdd, schema))
  }
}

