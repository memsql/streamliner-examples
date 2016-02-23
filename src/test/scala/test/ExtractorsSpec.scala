package test

import com.memsql.spark.etl.api.UserExtractConfig
import com.memsql.spark.etl.utils.ByteUtils
import com.memsql.streamliner.examples._
import org.apache.spark.streaming.{Time, Duration, StreamingContext}
import java.io._
import spray.json._
import test.util.{UnitSpec, TestLogger, LocalSparkContext}
import org.apache.spark.streaming._
import org.apache.spark.sql.{SQLContext, Row}

class ExtractorsSpec extends UnitSpec with LocalSparkContext {
  val emptyConfig = UserExtractConfig(class_name = "Test", value = new JsString("empty"))
  val logger = new TestLogger("test")

  var ssc: StreamingContext = _
  var sqlContext: SQLContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = new StreamingContext(sc, Seconds(1))
    sqlContext = new SQLContext(sc)
  }

  "ConstantExtractor" should "emit a constant DataFrame" in {
    val extract = new ConstantExtractor
    
    val maybeDf = extract.next(ssc, 1, sqlContext, emptyConfig, 1, logger)
    assert(maybeDf.isDefined)

    val total = maybeDf.get.select("number").map(r => r(0).asInstanceOf[Int]).sum()
    assert(total == 15)
  }

  "ConfigurableConstantExtractor" should "emit what the user specifies" in {
    val extract = new ConfigurableConstantExtractor

    val columnName = "mycolumn"
    val config = UserExtractConfig(
      class_name="test",
      value=JsObject(
        "start" -> JsNumber(1),
        "end" -> JsNumber(3),
        "column_name" -> JsString(columnName)
      )
    )

    val maybeDf = extract.next(ssc, 1, sqlContext, config, 1, logger)
    assert(maybeDf.isDefined)

    val total = maybeDf.get.select(columnName).map(r => r(0).asInstanceOf[Int]).sum()
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

    extract.initialize(ssc, sqlContext, config, 1, logger)

    var i = 0
    for (i <- 1 to 3) {
      val maybeDf = extract.next(ssc, 1, sqlContext, config, 1, logger)
      assert(maybeDf.isDefined)

      val rdd = maybeDf.get.select("number").map(r => r(0).asInstanceOf[Int])
      assert(rdd.count == 1)
      assert(rdd.first == i)
    }

    extract.cleanup(ssc, sqlContext, config, 1, logger)    
  }

  "FileExtractor" should "produce DataFrame from files" in {
    val extract = new FileExtractor

    // initialize the extractor
    val tweetsURI = getClass.getResource("/tweets").toURI
    val config = UserExtractConfig(
      class_name = "test",
      value = JsObject(
        "path" -> JsString(tweetsURI.toURL.toString)
      )
    )
    extract.initialize(ssc, sqlContext, config, 1, logger)

    // extract data
    val maybeDf = extract.next(ssc, 1, sqlContext, config, 1, logger)
    assert(maybeDf.isDefined)

    val df = maybeDf.get
    assert(df.count == 312)
  }
}
