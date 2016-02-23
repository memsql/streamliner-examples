package test

import com.memsql.spark.connector.MemSQLContext
import com.memsql.spark.etl.api.{UserTransformConfig, UserExtractConfig}
import com.memsql.spark.examples.avro.{AvroTransformer, AvroRandomExtractor}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import test.util.{Fixtures, UnitSpec, LocalSparkContext}
import spray.json._

class AvroTransformerSpec extends UnitSpec with LocalSparkContext {
  var ssc: StreamingContext = _
  var msc: MemSQLContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = new StreamingContext(sc, Seconds(1))
    msc = new MemSQLContext(sc)
  }

  val avroConfig = Fixtures.avroConfig.parseJson
  val extractConfig = UserExtractConfig(class_name = "Test", value = avroConfig)
  val transformConfig = UserTransformConfig(class_name = "Test", value = avroConfig)

  "AvroRandomTransformer" should "emit a dataframe of properly deserialized data" in {
    val extractor = new AvroRandomExtractor
    val transformer = new AvroTransformer

    extractor.initialize(null, null, extractConfig, 0, null)
    transformer.initialize(null, transformConfig, null)

    val maybeDf = extractor.next(null, 0, msc, null, 0, null)
    assert(maybeDf.isDefined)
    val extractedDf = maybeDf.get

    val transformedDf = transformer.transform(msc, extractedDf, null, null)

    val rows = transformedDf.collect()
    for (row <- rows) {
      assert(row(0).isInstanceOf[Boolean])
      assert(row(1).isInstanceOf[Double])
      assert(row(2).isInstanceOf[Float])
      assert(row(3).isInstanceOf[Int])
      assert(row(4).isInstanceOf[Long])
      assert(row(5) === null)
      assert(row(6).isInstanceOf[String])
      assert(row(7).isInstanceOf[String])
    }
  }
}
