package test

import com.memsql.spark.etl.api.UserTransformConfig
import com.memsql.spark.etl.utils.ByteUtils
import com.memsql.streamliner.examples._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, IntegerType, StructType}
import spray.json.{JsBoolean, JsObject, JsString}
import test.util.{UnitSpec, TestLogger, LocalSparkContext}

class TransformersSpec extends UnitSpec with LocalSparkContext {
  val emptyConfig = UserTransformConfig(class_name = "Test", value = JsString("empty"))
  val logger = new TestLogger("test")

  var sqlContext: SQLContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    sqlContext = new SQLContext(sc)
  }

  "EvenNumbersOnlyTransformer" should "only emit even numbers" in {
    val transform = new EvenNumbersOnlyTransformer
    val rdd = sc.parallelize(List(1,2,3).map(ByteUtils.intToBytes))

    val df = transform.transform(sqlContext, rdd, emptyConfig, logger)
    assert(df.schema == StructType(Array(StructField("number", IntegerType, true))))
    assert(df.first == Row(2))
    assert(df.count == 1)
  }

  "ConfigurableNumberParityTransformer" should "support skipping odd numbers" in {
    val transform = new ConfigurableNumberParityTransformer
    val rdd = sc.parallelize(List(1,2,3).map(ByteUtils.intToBytes))

    val config = UserTransformConfig(
      class_name="test",
      value=JsObject("filter" -> JsObject("odd" -> JsBoolean(false)))
    )

    val df = transform.transform(sqlContext, rdd, config, logger)
    assert(df.schema == StructType(Array(StructField("number", IntegerType, true))))
    assert(df.first == Row(2))
    assert(df.count == 1)
  }

  it should "support skipping even numbers" in {
    val transform = new ConfigurableNumberParityTransformer
    val rdd = sc.parallelize(List(1,2,3).map(ByteUtils.intToBytes))

    val config = UserTransformConfig(
      class_name="test",
      value=JsObject("filter" -> JsObject( "even" -> JsBoolean(false) ))
    )

    val df = transform.transform(sqlContext, rdd, config, logger)
    assert(df.schema == StructType(Array(StructField("number", IntegerType, true))))
    assert(df.first == Row(1))
    assert(df.count == 2)
  }

  it should "handle an empty filter" in {
    val transform = new ConfigurableNumberParityTransformer
    val rdd = sc.parallelize(List(1,2,3).map(ByteUtils.intToBytes))

    val config = UserTransformConfig(
      class_name="test",
      value=JsObject("filter" -> JsObject())
    )

    val df = transform.transform(sqlContext, rdd, config, logger)
    assert(df.schema == StructType(Array(StructField("number", IntegerType, true))))
    assert(df.first == Row(1))
    assert(df.count == 3)
  }

  it should "support setting a custom column" in {
    val transform = new ConfigurableNumberParityTransformer
    val rdd = sc.parallelize(List(1,2,3).map(ByteUtils.intToBytes))

    val config = UserTransformConfig(
      class_name="test",
      value=JsObject("table" -> JsObject("column_name" -> JsString("test")))
    )

    val df = transform.transform(sqlContext, rdd, config, logger)
    assert(df.schema == StructType(Array(StructField("test", IntegerType, true))))
    assert(df.first == Row(1))
    assert(df.count == 3)
  }
}
