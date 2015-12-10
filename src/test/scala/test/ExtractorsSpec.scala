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

  "DStreamExtractor" should "produce DataFrames from the InputDStream" in {
    val extract = new DStreamExtractor

    // create tmp directory
    val dataDir = System.getProperty("java.io.tmpdir") + "dstream/"
    try {
      val dir = new File(dataDir)
      dir.mkdir()
    }
    finally {
      System.setProperty("java.io.tmpdir", dataDir)
    }

    // initialize the extractor
    val config = UserExtractConfig(
      class_name = "test",
      value = JsObject(
        "dataDir" -> JsString(dataDir)
      )
    )
    extract.initialize(ssc, sqlContext, config, 1, logger)

    // wait some time, the check for new files is time sensitive
    Thread sleep 1000

    // create a new file, write data, rename the file to trigger the new file check
    val file = File.createTempFile("tmp", ".txt")
    val writer = new FileWriter(file)
    try {
      writer.write("hello world\nhello foo bar\nbar world")
    }
    finally writer.close()
    file.renameTo(new File(dataDir, "testfile.txt"))

    // extract data
    val maybeDf = extract.next(ssc, System.currentTimeMillis(), sqlContext, config, 1, logger)
    assert(maybeDf.isDefined)

    // test that the dstream successfully created a dataframe
    // http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
    maybeDf.get.registerTempTable("words")
    val wordCountsDataFrame = sqlContext.sql("select word, count(*) as total from words group by word order by word")
    assert(wordCountsDataFrame.count == 4)
    assert(wordCountsDataFrame.head == Row("bar", 2))
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
