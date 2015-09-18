package com.memsql.spark.examples.thrift

import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types._
import org.scalatest._

class ThriftToSchemaSpec extends FlatSpec {
  "ThriftToSchema" should "create a schema from Thrift classes" in {
    val schema = ThriftToSchema.getSchema(classOf[TestClass])
    val fields = schema.fields
    assert(fields.forall(field => field.nullable))
    assert(fields(0).name == "bool_value")
    assert(fields(0).dataType == BooleanType)
    assert(fields(1).name == "byte_value")
    assert(fields(1).dataType == ByteType)
    assert(fields(2).name == "i16_value")
    assert(fields(2).dataType == ShortType)
    assert(fields(3).name == "i32_value")
    assert(fields(3).dataType == IntegerType)
    assert(fields(4).name == "i64_value")
    assert(fields(4).dataType == LongType)
    assert(fields(5).name == "double_value")
    assert(fields(5).dataType == DoubleType)
    assert(fields(6).name == "string_value")
    assert(fields(6).dataType == StringType)
    assert(fields(7).name == "binary_value")
    assert(fields(7).dataType == BinaryType)
    assert(fields(8).name == "map_value")
    assert(fields(8).dataType == JsonType)
    assert(fields(9).name == "list_value")
    assert(fields(9).dataType == JsonType)
    assert(fields(10).name == "set_value")
    assert(fields(10).dataType == JsonType)
    assert(fields(11).name == "enum_value")
    assert(fields(11).dataType == IntegerType)
    assert(fields(12).name == "sub_class_value")
    assert(fields(12).dataType == JsonType)
  }
}
