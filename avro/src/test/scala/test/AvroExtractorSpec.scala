package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api.UserExtractConfig
import test.util.{UnitSpec, TestLogger, LocalSparkContext}
import spray.json._

class ExtractorsSpec extends UnitSpec with LocalSparkContext {
    val avroJsonSchemaTest = """
      {
        "avroSchema": {
          "namespace": "com.memsql.spark.examples.avro",
          "type": "record",
          "name": "TestSchema",
          "fields": [
            {
              "name": "testBool",
              "type": "boolean"
            },
            {
              "name": "testDouble",
              "type": "double"
            },
            {
              "name": "testFloat",
              "type": "float"
            },
            {
              "name": "testInt",
              "type": "int"
            },
            {
              "name": "testLong",
              "type": "long"
            },
            {
              "name": "testNull",
              "type": "null"
            },
            {
              "name": "testString",
              "type": "string"
            }
          ]
        }
      }
  """.parseJson

  val avroConfig = UserExtractConfig(class_name = "Test", value = avroJsonSchemaTest)
  val logger = new TestLogger("test")

  "AvroRandomExtractor" should "emit a random RDD" in {
    val extract = new AvroRandomExtractor
    extract.initialize(sc, avroConfig, 1, logger)
    val maybeRDD = extract.nextRDD(sc, avroConfig, 1, logger)

    assert(maybeRDD.isDefined)
  }
}
