package com.memsql.spark.examples.avro

import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types._
import org.apache.avro.Schema
import org.scalatest._
import test.util.Fixtures

class AvroToSchemaSpec extends FlatSpec {
  "AvroToSchema" should "create a Spark SQL schema from an Avro schema" in {
    val parser = new Schema.Parser()
    val avroSchema = parser.parse(Fixtures.avroSchema)
    val sparkSchema = AvroToSchema.getSchema(avroSchema)
    val fields = sparkSchema.fields

    assert(fields.forall(field => field.nullable))
    assert(fields(0).name == "testBool")
    assert(fields(0).dataType == BooleanType)

    assert(fields(1).name == "testDouble")
    assert(fields(1).dataType == DoubleType)

    assert(fields(2).name == "testFloat")
    assert(fields(2).dataType == FloatType)

    assert(fields(3).name == "testInt")
    assert(fields(3).dataType == IntegerType)

    assert(fields(4).name == "testLong")
    assert(fields(4).dataType == LongType)

    assert(fields(5).name == "testNull")
    assert(fields(5).dataType == NullType)

    assert(fields(6).name == "testString")
    assert(fields(6).dataType == StringType)

    assert(fields(7).name == "testUnion")
    assert(fields(7).dataType == StringType)
  }
}
