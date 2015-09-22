package test

import com.memsql.spark.etl.api.UserExtractConfig
import com.memsql.spark.etl.utils.ByteUtils
import com.memsql.streamliner.examples._
import spray.json._
import test.util.{UnitSpec, TestLogger, LocalSparkContext}

class ExtractorsSpec extends UnitSpec with LocalSparkContext {
  val emptyConfig = UserExtractConfig(class_name = "Test", value = new JsString("empty"))
  val logger = new TestLogger("test")

  "ConstantExtractor" should "emit a constant RDD" in {
    val extract = new ConstantExtractor
    val maybeRDD = extract.nextRDD(sc, emptyConfig, 1, logger)

    assert(maybeRDD.isDefined)

    val total = maybeRDD.get.map(ByteUtils.bytesToInt).sum()
    assert(total == 15)
  }

  "ConfigurableConstantExtractor" should "emit what the user specifies" in {
    val extract = new ConfigurableConstantExtractor

    val config = UserExtractConfig(
      class_name="test",
      value=JsObject(
        "start" -> JsNumber(1),
        "end" -> JsNumber(3)
      )
    )

    val maybeRDD = extract.nextRDD(sc, config, 1, logger)
    assert(maybeRDD.isDefined)

    val total = maybeRDD.get.map(ByteUtils.bytesToInt).sum()
    assert(total == 6)
  }

  "SequenceExtractor" should "maintain sequence state" in {
    val extract = new SequenceExtractor

    val config = UserExtractConfig(
      class_name = "test",
      value = JsObject(
        "sequence" -> JsObject(
          "initial_value" -> JsNumber(1),
          "size" -> JsNumber(1)
        )
      )
    )

    extract.initialize(sc, config, 1, logger)

    var i = 0
    for (i <- 1 to 3) {
      val maybeRDD = extract.nextRDD(sc, config, 1, logger)
      assert(maybeRDD.isDefined)
      val rdd = maybeRDD.get.map(ByteUtils.bytesToInt)
      assert(rdd.count == 1)
      assert(rdd.first == i)
    }
  }
}
