package com.memsql.spark.examples.avro

import org.scalatest._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import test.util.Fixtures

class AvroRandomGeneratorSpec extends FlatSpec {
  "AvroRandomGenerator" should "create Avro objects with random values" in {
    val schema = new Schema.Parser().parse(Fixtures.avroSchema)
    val avroRecord:GenericData.Record = new AvroRandomGenerator(schema).next().asInstanceOf[GenericData.Record]

    assert(avroRecord.get("testBool").isInstanceOf[Boolean])
    assert(avroRecord.get("testDouble").isInstanceOf[Double])
    assert(avroRecord.get("testFloat").isInstanceOf[Float])
    assert(avroRecord.get("testInt").isInstanceOf[Int])
    assert(avroRecord.get("testLong").isInstanceOf[Long])
    assert(avroRecord.get("testNull") == null)
    assert(avroRecord.get("testString").isInstanceOf[String])
    assert(avroRecord.get("testUnion").isInstanceOf[Int])
  }
}
