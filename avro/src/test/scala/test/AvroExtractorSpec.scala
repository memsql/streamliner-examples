package com.memsql.spark.examples.avro

import com.memsql.spark.etl.api.UserExtractConfig
import org.apache.spark.streaming._
import org.apache.spark.sql.SQLContext
import test.util.{Fixtures, UnitSpec, TestLogger, LocalSparkContext}
import spray.json._

class ExtractorsSpec extends UnitSpec with LocalSparkContext {
  var ssc: StreamingContext = _
  var sqlContext: SQLContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = new StreamingContext(sc, Seconds(1))
    sqlContext = new SQLContext(sc)
  }

  val avroConfig = Fixtures.avroConfig.parseJson
  val extractConfig = UserExtractConfig(class_name = "Test", value = avroConfig)
  val logger = new TestLogger("test")

  "AvroRandomExtractor" should "emit a random DF" in {
    val extract = new AvroRandomExtractor
    extract.initialize(ssc, sqlContext, extractConfig, 1, logger)

    val maybeDf = extract.next(ssc, 1, sqlContext, extractConfig, 1, logger)
    assert(maybeDf.isDefined)
    assert(maybeDf.get.count == 5)
  }
}
