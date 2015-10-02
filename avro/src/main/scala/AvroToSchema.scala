package com.memsql.spark.examples.avro

import collection.JavaConversions._
import com.memsql.spark.connector.dataframe.JsonType
import org.apache.spark.sql.types._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

private object AvroToSchema {
  def getSchema(schema: Schema): StructType = {
    StructType(schema.getFields.map(field => {
      val fieldName = field.name
      val fieldSchema = field.schema
      val fieldType = fieldSchema.getType match {
        case Schema.Type.BOOLEAN => BooleanType
        case Schema.Type.DOUBLE => DoubleType
        case Schema.Type.ENUM => StringType
        case Schema.Type.FLOAT => FloatType
        case Schema.Type.INT => IntegerType
        case Schema.Type.LONG => LongType
        case Schema.Type.NULL => StringType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType.asInstanceOf[DataType], true)
    }))
  }
}
