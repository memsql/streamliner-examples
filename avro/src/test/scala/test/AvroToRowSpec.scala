package com.memsql.spark.examples.avro

import com.memsql.spark.connector.dataframe.JsonValue

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row

import collection.JavaConversions._
import java.nio.ByteBuffer
import org.scalatest._

class AvroToRowSpec extends FlatSpec {
  "AvroToRow" should "create Spark SQL Rows from Avro objects" in {
    val avroTestJsonSchemaString:String = s"""
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
    val parser: Schema.Parser = new Schema.Parser()
    val avroTestSchema: Schema = parser.parse(avroTestJsonSchemaString)

    val record: GenericData.Record = new GenericData.Record(avroTestSchema)

    record.put("testBool", true)
    record.put("testDouble", 19.88)
    record.put("testFloat", 3.19f)
    record.put("testInt", 1123)
    record.put("testLong", 2147483648L)
    record.put("testNull", null)
    record.put("testString", "Conor")

    val row: Row = new AvroToRow().getRow(record)

    assert(row.getAs[Boolean](0))
    assert(row.getAs[Double](1) == 19.88)
    assert(row.getAs[Float](2) == 3.19f)
    assert(row.getAs[Int](3) == 1123)
    assert(row.getAs[Long](4) == 2147483648L)
    assert(row.getAs[Null](5) == null)
    assert(row.getAs[String](6) == "Conor")
  }
}


