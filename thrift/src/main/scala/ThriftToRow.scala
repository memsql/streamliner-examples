package com.memsql.spark.examples.thrift

import org.apache.thrift.TBase
import org.apache.thrift.protocol.TField
import org.apache.spark.sql.Row

private class ThriftToRow(c: Class[_]) {
  val fieldMembers = c.getDeclaredFields.filter(_.getType() == classOf[TField])
  val thriftIds = fieldMembers.map({ x =>
    x.setAccessible(true)
    x.get(null).asInstanceOf[TField].id
  })
  val protocol = new ThriftToRowSerializer(thriftIds)

  def getRow(t: TBase[_,_]): Row = {
    t.write(protocol)
    return protocol.getRow()
  }
}
