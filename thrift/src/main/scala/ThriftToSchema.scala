package com.memsql.spark.examples.thrift

import collection.JavaConversions._
import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types._
import org.apache.thrift.{TBase, TFieldIdEnum}
import org.apache.thrift.protocol.{TField, TType}
import org.apache.thrift.meta_data._

private object ThriftToSchema {
  def getSchema(c: Class[_]) : StructType = {
    val className = c.getName
    var tBaseClass: Class[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]] = null
    try {
      tBaseClass = c.asInstanceOf[Class[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]]
    } catch {
      case e: ClassCastException => throw new IllegalArgumentException(s"Class $className is not a subclass of org.apache.thrift.TBase")
    }
    val metaDataMap: Map[_ <: TFieldIdEnum, FieldMetaData] = FieldMetaData.getStructMetaDataMap(tBaseClass).toMap
    // Sort the fields by their thrift ID so that they're in a consistent
    // order.
    val metaDataSeq = metaDataMap.toSeq.sortBy(_._1.getThriftFieldId)
    StructType(metaDataSeq.map({ case (field, fieldMetaData) =>
      val fieldName = fieldMetaData.fieldName
      val fieldType = fieldMetaData.valueMetaData.`type` match {
        case TType.BOOL => BooleanType
        case TType.BYTE => ByteType
        case TType.I16 => ShortType
        case TType.I32 => IntegerType
        case TType.I64 => LongType
        case TType.DOUBLE => DoubleType
        case TType.STRING => {
          if (fieldMetaData.valueMetaData.isBinary) {
            BinaryType
          } else {
            StringType
          }
        }
        case TType.ENUM => IntegerType
        case _ => JsonType
      }
      StructField(fieldName, fieldType, true)
    }))
  }
}
