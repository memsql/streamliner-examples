package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api.UserExtractConfig
import org.apache.spark.streaming._
import org.apache.spark.sql.SQLContext
import org.apache.avro.generic.GenericData
import test.util.{UnitSpec, TestLogger, LocalSparkContext}
import spray.json._

class ExtractorsSpec extends UnitSpec with LocalSparkContext {
  var ssc: StreamingContext = _
  var sqlContext: SQLContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = new StreamingContext(sc, Seconds(1))
    sqlContext = new SQLContext(sc)
  }

  val avroJsonSchemaTest = """
    {
      "count": 5,
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

  "AvroRandomExtractor" should "emit a random DF" in {
    val extract = new AvroRandomExtractor
    extract.initialize(ssc, sqlContext, avroConfig, 1, logger)

    val maybeDf = extract.next(ssc, 1, sqlContext, avroConfig, 1, logger)
    assert(maybeDf.isDefined)
    assert(maybeDf.get.count == 5)
  }
}
