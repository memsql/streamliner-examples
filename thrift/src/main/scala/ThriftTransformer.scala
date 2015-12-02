package com.memsql.spark.examples.thrift

import com.memsql.spark.etl.api._
import com.memsql.spark.etl.utils.PhaseLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum}

class ThriftTransformer extends Transformer {
  private var classObj: Class[_] = null
  private var thriftToRow: ThriftToRow = null
  private var deserializer: TDeserializer = null
  private var schema: StructType = null

  def thriftRDDToDataFrame(sqlContext: SQLContext, rdd: RDD[Row]): DataFrame = {
    val rowRDD: RDD[Row] = rdd.map({ record =>
      val recordAsBytes = record(0).asInstanceOf[Array[Byte]]
      val i = classObj.newInstance().asInstanceOf[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]
      deserializer.deserialize(i, recordAsBytes)
      thriftToRow.getRow(i)
    })
    sqlContext.createDataFrame(rowRDD, schema)
  }

  override def initialize(sqlContext: SQLContext, config: PhaseConfig, logger: PhaseLogger): Unit = {
    val userConfig = config.asInstanceOf[UserTransformConfig]
    val className = userConfig.getConfigString("className") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("className must be set in the config")
    }

    classObj = Class.forName(className)
    thriftToRow = new ThriftToRow(classObj)
    deserializer = new TDeserializer()

    schema = ThriftToSchema.getSchema(classObj)
  }

  override def transform(sqlContext: SQLContext, df: DataFrame, config: PhaseConfig, logger: PhaseLogger): DataFrame = {
    thriftRDDToDataFrame(sqlContext, df.rdd)
  }
}
