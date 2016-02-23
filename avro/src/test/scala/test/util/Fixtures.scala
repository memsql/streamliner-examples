package test.util

object Fixtures {

  val avroSchema = s"""
      {
        "namespace": "com.memsql.spark.examples.avro",
        "type": "record",
        "name": "TestSchema",
        "fields": [
          {
            "name": "testBool",
            "type": "boolean"
          },
          {
            "name": "testDouble",
            "type": "double"
          },
          {
            "name": "testFloat",
            "type": "float"
          },
          {
            "name": "testInt",
            "type": "int"
          },
          {
            "name": "testLong",
            "type": "long"
          },
          {
            "name": "testNull",
            "type": "null"
          },
          {
            "name": "testString",
            "type": "string"
          },
          {
            "name": "testUnion",
            "type": [
              "int",
              "string",
              "null"
            ]
          }
        ]
      }
    """


val avroConfig = s"""
      {
        "count": 5,
        "avroSchema": $avroSchema
      }
     """
}
