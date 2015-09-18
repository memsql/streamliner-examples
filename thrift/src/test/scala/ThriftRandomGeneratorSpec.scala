package com.memsql.spark.examples.thrift

import org.scalatest._
import org.apache.thrift.{TBase, TFieldIdEnum}

class ThriftRandomGeneratorSpec extends FlatSpec {
  "ThriftRandomGenerator" should "create Thrift objects with random values" in {
    val thriftType = classOf[TestClass]
    val thriftObject = ThriftRandomGenerator.next(thriftType).asInstanceOf[TestClass]
    assert(thriftObject.string_value != null)
    assert(thriftObject.binary_value != null)
    assert(thriftObject.list_value.size > 0)
    assert(thriftObject.list_value.get(0).isInstanceOf[String])
    assert(thriftObject.set_value.size > 0)
    assert(thriftObject.set_value.toArray()(0).isInstanceOf[String])
    assert(thriftObject.map_value.size > 0)
    val keys = thriftObject.map_value.keySet.toArray
    assert(keys(0).isInstanceOf[String])
    assert(thriftObject.map_value.get(keys(0)).isInstanceOf[String])
    val testEnumValues = Set(TestEnum.FIRST_VALUE, TestEnum.SECOND_VALUE)
    assert(testEnumValues.contains(thriftObject.enum_value))
    assert(thriftObject.sub_class_value != null)
    assert(thriftObject.sub_class_value.string_value != null)
  }
}
