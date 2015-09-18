package com.memsql.spark.examples.thrift

import collection.JavaConversions._
import java.lang.reflect.Method
import java.nio.ByteBuffer
import org.apache.thrift.{TBase, TFieldIdEnum}
import org.apache.thrift.protocol.{TField, TType}
import org.apache.thrift.meta_data._

import scala.util.Random

object ThriftRandomGenerator {
  val random = new Random
  val MAX_RECURSION_LEVEL = 5

  def next[F <: TFieldIdEnum](c: Class[_], level: Int = 0): Any = {
    if (level > MAX_RECURSION_LEVEL) {
      return null
    }
    val className = c.getName
    try {
      val tBaseClass = c.asInstanceOf[Class[TBase[_ <: TBase[_, _], F]]]
      val instance = tBaseClass.newInstance()
      val metaDataMap: Map[_ <: TFieldIdEnum, FieldMetaData] = FieldMetaData.getStructMetaDataMap(tBaseClass).toMap
      metaDataMap.foreach({ case (field, fieldMetaData) =>
        val valueMetaData = fieldMetaData.valueMetaData
        val value = getValue(valueMetaData, level)
        instance.setFieldValue(instance.fieldForId(field.getThriftFieldId), value)
      })
      instance
    } catch {
      case e: ClassCastException => throw new IllegalArgumentException(s"Class $className is not a subclass of org.apache.thrift.TBase")
    }
  }

  def getValue(valueMetaData: FieldValueMetaData, level: Int): Any = {
    if (level > MAX_RECURSION_LEVEL) {
      return null
    }
    valueMetaData.`type` match {
      case TType.BOOL => random.nextBoolean
      case TType.BYTE => random.nextInt.toByte
      case TType.I16 => random.nextInt.toShort
      case TType.I32 => random.nextInt
      case TType.I64 => random.nextLong
      case TType.DOUBLE => random.nextInt(5) * 0.25
      case TType.ENUM => {
        val enumClass = valueMetaData.asInstanceOf[EnumMetaData].enumClass
        getEnumValue(enumClass)
      }
      case TType.STRING => {
        val length: Int = 5 + random.nextInt(5)
        val s = (1 to length).map(x => ('a'.toInt + random.nextInt(26)).toChar).mkString
        if (valueMetaData.isBinary) {
          ByteBuffer.wrap(s.getBytes)
        } else {
          s
        }
      }
      case TType.LIST => {
        val elemMetaData = valueMetaData.asInstanceOf[ListMetaData].elemMetaData
        val length: Int = 5 + random.nextInt(5)
        val ret: java.util.List[Any] = (1 to length).map(x => getValue(elemMetaData, level + 1))
        ret
      }
      case TType.SET => {
        val elemMetaData = valueMetaData.asInstanceOf[SetMetaData].elemMetaData
        val length: Int = 5 + random.nextInt(5)
        val ret: Set[Any] = (1 to length).map(x => getValue(elemMetaData, level + 1)).toSet
        val javaSet: java.util.Set[Any] = ret
        javaSet
      }
      case TType.MAP => {
        val mapMetaData = valueMetaData.asInstanceOf[MapMetaData]
        val keyMetaData = mapMetaData.keyMetaData
        val mapValueMetaData = mapMetaData.valueMetaData
        val length: Int = 5 + random.nextInt(5)
        val ret: Map[Any, Any] = (1 to length).map(_ => {
          val mapKey = getValue(keyMetaData, level + 1)
          val mapValue = getValue(mapValueMetaData, level + 1)
          mapKey -> mapValue
        }).toMap
        val javaMap: java.util.Map[Any, Any] = ret
        javaMap
      }
      case TType.STRUCT => {
        val structClass = valueMetaData.asInstanceOf[StructMetaData].structClass
        next(structClass, level = level + 1)
      }
      case _ => null
    }
  }

  def getEnumValue(enumType: Class[_]): Any = {
    val enumConstants = enumType.getEnumConstants
    enumConstants(random.nextInt(enumConstants.length))
  }
}
