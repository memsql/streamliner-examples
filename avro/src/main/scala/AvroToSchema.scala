package com.memsql.spark.examples.avro

import collection.JavaConversions._
import org.apache.spark.sql.types._
import org.apache.avro.Schema

// Converts an Avro schema to a Spark DataFrame schema.
//
// This assumes that the Avro schema is "flat", i.e. a Record that includes primitive types
// or unions of primitive types. Unions, and Avro types that don't directly map to Scala types,
// are converted to Strings and put in a Spark SQL StringType column.
private object AvroToSchema {
  def getSchema(schema: Schema): StructType = {
    StructType(schema.getFields.map(field => {
      val fieldName = field.name
      val fieldSchema = field.schema
      val fieldType = fieldSchema.getType match {
        case Schema.Type.BOOLEAN => BooleanType
        case Schema.Type.DOUBLE => DoubleType
        case Schema.Type.FLOAT => FloatType
        case Schema.Type.INT => IntegerType
        case Schema.Type.LONG => LongType
        case Schema.Type.NULL => NullType
        case Schema.Type.STRING => StringType
        case _ => StringType
      }
      StructField(fieldName, fieldType.asInstanceOf[DataType], true)
    }))
  }
}
