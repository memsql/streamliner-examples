package com.memsql.spark.examples.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.collection.JavaConversions._
import scala.util.Random

class AvroRandomGenerator(inSchema: Schema) {
  // Avoid nested Records, since our destination is a DataFrame.
  val MAX_RECURSION_LEVEL: Int = 1

  val topSchema: Schema = inSchema
  val random = new Random

  def next(schema: Schema = this.topSchema, level: Int = 0): Any = {
    if (level <= MAX_RECURSION_LEVEL) {

      schema.getType match {
        case Schema.Type.RECORD => {
          val datum = new GenericData.Record(schema)
          schema.getFields.foreach {
            x => datum.put(x.pos, next(x.schema, level + 1))
          }
          datum
        }

        case Schema.Type.UNION => {
          val types = schema.getTypes
          // Generate a value using the first type in the union.
          // "Random type" is also a valid option.
          next(types(0), level)
        }

        case _ => generateValue(schema.getType)
      }

    } else {
      null
    }
  }

  def generateValue(avroType: Schema.Type): Any = avroType match {
    case Schema.Type.BOOLEAN => random.nextBoolean
    case Schema.Type.DOUBLE => random.nextDouble
    case Schema.Type.FLOAT => random.nextFloat
    case Schema.Type.INT => random.nextInt
    case Schema.Type.LONG => random.nextLong
    case Schema.Type.NULL => null
    case Schema.Type.STRING => getRandomString
    case _ => null
  }

  def getRandomString(): String = {
    val length: Int = 5 + random.nextInt(5)
    (1 to length).map(x => ('a'.toInt + random.nextInt(26)).toChar).mkString
  }

}
