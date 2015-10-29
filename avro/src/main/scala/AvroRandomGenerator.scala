package com.memsql.spark.examples.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.collection.JavaConversions._
import scala.util.Random

class AvroRandomGenerator(inSchema: Schema) {
  val MAX_RECURSION_LEVEL: Int = 1
  val topSchema: Schema = inSchema
  val random = new Random

  def next(schema: Schema = this.topSchema, level: Int = 0): Any = {
    if (level > MAX_RECURSION_LEVEL) {
      return null
    }

    schema.getType match {
      case Schema.Type.BOOLEAN => random.nextBoolean
      case Schema.Type.DOUBLE => random.nextDouble
      case Schema.Type.FLOAT => random.nextFloat
      case Schema.Type.INT => random.nextInt
      case Schema.Type.LONG => random.nextLong
      case Schema.Type.NULL => null
      case Schema.Type.RECORD => {
        val datum = new GenericData.Record(schema)
        schema.getFields.map{x => datum.put(x.pos, next(x.schema, level+1))}
        datum
      }
      case Schema.Type.STRING => getRandomString
      case _ => null
    }
  }

  def getRandomString(): String = {
    val length: Int = 5 + random.nextInt(5)
    (1 to length).map(x => ('a'.toInt + random.nextInt(26)).toChar).mkString
  }

}
