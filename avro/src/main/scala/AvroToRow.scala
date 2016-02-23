package com.memsql.spark.examples.avro

import collection.JavaConversions._
import org.apache.spark.sql.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

// Converts an Avro record to a Spark DataFrame row.
//
// This assumes that the Avro schema is "flat", i.e. a Record that includes primitive types
// or unions of primitive types. Unions, and Avro types that don't directly map to Scala types,
// are converted to Strings and put in a Spark SQL StringType column.
private class AvroToRow {
  def getRow(record: GenericData.Record): Row = {
    Row.fromSeq(record.getSchema.getFields().map(f => {
      val schema = f.schema()
      val obj = record.get(f.pos)

      schema.getType match {
        case Schema.Type.BOOLEAN => obj.asInstanceOf[Boolean]
        case Schema.Type.DOUBLE => obj.asInstanceOf[Double]
        case Schema.Type.FLOAT => obj.asInstanceOf[Float]
        case Schema.Type.INT => obj.asInstanceOf[Int]
        case Schema.Type.LONG => obj.asInstanceOf[Long]
        case Schema.Type.NULL => null

        case _ => obj.toString
      }
    }))
  }
}
