package com.memsql.spark.examples.thrift

import com.memsql.spark.etl.api.SimpleByteArrayTransformer
import com.memsql.spark.etl.api.configs.UserTransformConfig
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.thrift.{TBase, TDeserializer, TFieldIdEnum}

class ThriftTransformer extends SimpleByteArrayTransformer {
  def thriftRDDToDataFrame(sqlContext: SQLContext, rdd: RDD[Array[Byte]], c: Class[_]): DataFrame = {
    val thriftToRow = new ThriftToRow(c)
    val deserializer = new TDeserializer()
    val rowRDD: RDD[Row] = rdd.map({ record =>
      val i = c.newInstance().asInstanceOf[TBase[_ <: TBase[_, _], _ <: TFieldIdEnum]]
      deserializer.deserialize(i, record)
      thriftToRow.getRow(i)
    })
    sqlContext.createDataFrame(rowRDD, ThriftToSchema.getSchema(c))
  }

  override def transform(sqlContext: SQLContext, rdd: RDD[Array[Byte]], config: UserTransformConfig, logger: Logger): DataFrame = {
    val className = config.getConfigString("className") match {
      case Some(s) => s
      case None => throw new IllegalArgumentException("className must be set in the config")
    }
    thriftRDDToDataFrame(sqlContext, rdd, Class.forName(className))
  }
}
