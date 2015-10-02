package com.memsql.spark.examples.avro

import collection.JavaConversions._
import com.memsql.spark.etl.api.{UserExtractConfig, SimpleByteArrayExtractor}
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

import java.io.ByteArrayOutputStream

class AvroRandomExtractor extends SimpleByteArrayExtractor {
  var count: Int = 1
  val parser: Schema.Parser = new Schema.Parser()
  var generator: AvroRandomGenerator = null
  var writer: DatumWriter[GenericData.Record] = null
  var avroSchema: Schema = null

  override def initialize(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Unit = {
    val avroSchemaJson = config.getConfigJsValue("avroSchema") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("avroSchema must be set in the config")
    }
    count = config.getConfigInt("count").getOrElse(1)
    avroSchema = parser.parse(avroSchemaJson.toString)

    writer = new SpecificDatumWriter(avroSchema)
    generator = new AvroRandomGenerator(avroSchema)
  }

  override def nextRDD(sparkContext: SparkContext, config: UserExtractConfig, batchInterval: Long, logger: PhaseLogger): Option[RDD[Array[Byte]]] = {
    val rdd = sparkContext.parallelize((1 to count).map(_ => {
      val out = new ByteArrayOutputStream
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      val avroRecord: GenericData.Record = generator.next().asInstanceOf[GenericData.Record]

      writer.write(avroRecord, encoder)
      encoder.flush
      out.close
      out.toByteArray
    }))

    Some(rdd)
  }
}

