package com.memsql.spark.examples.avro

import collection.JavaConversions._
import org.apache.spark.sql.Row
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

private class AvroToRow {

  def getRow(record: GenericData.Record): Row = {
    Row.fromSeq(record.getSchema.getFields().map(f => {
      record.get(f.pos)
    }))
  }

}
