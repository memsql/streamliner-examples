package com.memsql.spark.examples.avro

import org.scalatest._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class AvroRandomGeneratorSpec extends FlatSpec {
  "AvroRandomGenerator" should "create Avro objects with random values" in {
    val avroJsonSchemaStringTest = s"""
      {
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
    """

    val schema = new Schema.Parser().parse(avroJsonSchemaStringTest)
    val avroRecord:GenericData.Record = new AvroRandomGenerator(schema).next().asInstanceOf[GenericData.Record]

    assert(avroRecord.get("testBool").isInstanceOf[Boolean])
    assert(avroRecord.get("testDouble").isInstanceOf[Double])
    assert(avroRecord.get("testFloat").isInstanceOf[Float])
    assert(avroRecord.get("testInt").isInstanceOf[Int])
    assert(avroRecord.get("testLong").isInstanceOf[Long])
    assert(avroRecord.get("testNull") == null)
    assert(avroRecord.get("testString").isInstanceOf[String])
  }
}
