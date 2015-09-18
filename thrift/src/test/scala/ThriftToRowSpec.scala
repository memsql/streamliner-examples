package com.memsql.spark.examples.thrift

import com.memsql.spark.connector.dataframe.JsonValue
import collection.JavaConversions._
import java.nio.ByteBuffer
import org.scalatest._

class ThriftToRowSpec extends FlatSpec {
  "ThriftToRow" should "create Spark SQL Rows from Thrift objects" in {
    val testClassInstance = new TestClass(
      true,
      42.toByte,
      128.toShort,
      1024,
      2048.toLong,
      2.5,
      "test1",
      ByteBuffer.wrap("test2".getBytes),
      mapAsJavaMap(Map("value" -> "test3")).asInstanceOf[java.util.Map[String, String]],
      List("test4"),
      Set("test5"),
      TestEnum.FIRST_VALUE,
      new SubClass("test6")
    )
    val thriftToRow = new ThriftToRow(classOf[TestClass])
    val row = thriftToRow.getRow(testClassInstance)
    assert(row.getAs[Boolean](0))
    assert(row.getAs[Byte](1) == 42.toByte)
    assert(row.getAs[Short](2) == 128.toShort)
    assert(row.getAs[Int](3) == 1024)
    assert(row.getAs[Long](4) == 2048)
    assert(row.getAs[Double](5) == 2.5)
    assert(row.getAs[String](6) == "test1")
    assert(row.getAs[ByteBuffer](7) == ByteBuffer.wrap("test2".getBytes))
    val mapValue = row.getAs[JsonValue](8)
    assert(mapValue.value == "{\"value\":\"test3\"}")
    val listValue = row.getAs[JsonValue](9)
    assert(listValue.value == "[\"test4\"]")
    val setValue = row.getAs[JsonValue](10)
    assert(setValue.value == "[\"test5\"]")
    assert(row.getAs[Int](11) == TestEnum.FIRST_VALUE.getValue)
    val subClassValue = row.getAs[JsonValue](12)
    assert(subClassValue.value == "{\"string_value\":\"test6\"}")
  }
}
