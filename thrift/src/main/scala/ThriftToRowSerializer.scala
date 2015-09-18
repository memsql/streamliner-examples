package com.memsql.spark.examples.thrift

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.memsql.spark.connector.dataframe.JsonValue
import org.apache.spark.sql.Row
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol._
import org.apache.thrift.transport.TIOStreamTransport

import scala.collection.mutable.ListBuffer

private class ThriftToRowSerializer(val thriftIds: Array[Short]) extends TProtocol(null) {
  val values = new ListBuffer[Any]
  var nestingLevel: Int = 0

  var jsonProtocol: TSimpleJSONProtocol = null
  var outputStream: ByteArrayOutputStream = null
  var transport: TIOStreamTransport = null
  var serializer: TSerializer = null
  var valueWritten = true
  var currentFieldId = 0

  def getRow(): Row = {
    while (currentFieldId < thriftIds.length) {
      values.append(null)
      currentFieldId += 1
    }
    return Row.fromSeq(values)
  }

  def startJSON(): Unit = {
    this.outputStream = new ByteArrayOutputStream()
    this.transport = new TIOStreamTransport(outputStream)
    this.jsonProtocol = new TSimpleJSONProtocol(this.transport)
  }

  def saveJSON(): Unit = {
    values.append(new JsonValue(outputStream.toString()))
    jsonProtocol = null
    outputStream = null
    transport = null
  }

  override def writeStructBegin(struct: TStruct): Unit = {
    if (nestingLevel == 0) {
      currentFieldId = 0
    } else {
      jsonProtocol.writeStructBegin(struct)
    }
    nestingLevel += 1
  }

  override def writeStructEnd(): Unit = {
    nestingLevel -= 1
    if (nestingLevel > 0) {
      jsonProtocol.writeStructEnd()
    }
    if (nestingLevel == 1) {
      saveJSON()
    }
  }

  override def writeMapBegin(m: TMap): Unit = {
    nestingLevel += 1
    jsonProtocol.writeMapBegin(m)
  }

  override def writeMapEnd(): Unit = {
    nestingLevel -= 1
    jsonProtocol.writeMapEnd()
    if (nestingLevel == 1) {
      saveJSON()
    }
  }

  override def writeSetBegin(s: TSet): Unit = {
    nestingLevel += 1
    jsonProtocol.writeSetBegin(s)
  }

  override def writeSetEnd(): Unit = {
    nestingLevel -= 1
    jsonProtocol.writeSetEnd()
    if (nestingLevel == 1) {
      saveJSON()
    }
  }

  override def writeListBegin(l: TList): Unit = {
    nestingLevel += 1
    jsonProtocol.writeListBegin(l)
  }

  override def writeListEnd(): Unit = {
    nestingLevel -= 1
    jsonProtocol.writeListEnd()
    if (nestingLevel == 1) {
      saveJSON()
    }
  }

  override def writeFieldBegin(field: TField): Unit = {
    if (nestingLevel != 1) {
      jsonProtocol.writeFieldBegin(field)
    } else {
      while (thriftIds(currentFieldId) != field.id) {
        values.append(null)
        currentFieldId += 1
      }
      field.`type` match {
        case TType.LIST | TType.MAP | TType.SET | TType.STRUCT =>
          startJSON()
        case _ => { }
      }
    }
  }

  override def writeFieldStop(): Unit = {
    if (nestingLevel != 1) {
      jsonProtocol.writeFieldStop()
    }
  }

  override def writeFieldEnd(): Unit = {
    if (nestingLevel != 1) {
      jsonProtocol.writeFieldStop()
    } else {
      currentFieldId += 1
    }
  }

  override def writeString(str: String): Unit = {
    if (nestingLevel == 1) {
      values.append(str)
    } else {
      jsonProtocol.writeString(str)
    }
  }

  override def writeBool(b: Boolean): Unit = {
    if (nestingLevel == 1) {
      values.append(b)
    } else {
      jsonProtocol.writeBool(b)
    }
  }

  override def writeMessageBegin(tMessage: TMessage): Unit = {
    if (nestingLevel != 1) {
      jsonProtocol.writeMessageBegin(tMessage)
    }
  }

  override def writeMessageEnd(): Unit = {
    if (nestingLevel != 1) {
      jsonProtocol.writeMessageEnd()
    }
  }

  override def writeByte(i: Byte): Unit = {
    if (nestingLevel == 1) {
      values.append(i)
    } else {
      jsonProtocol.writeByte(i)
    }
  }

  override def writeI16(i: Short): Unit = {
    if (nestingLevel == 1) {
      values.append(i)
    } else {
      jsonProtocol.writeI16(i)
    }
  }

  override def writeI32(i: Int): Unit = {
    if (nestingLevel == 1) {
      values.append(i)
    } else {
      jsonProtocol.writeI32(i)
    }
  }

  override def writeI64(i: Long): Unit = {
    if (nestingLevel == 1) {
      values.append(i)
    } else {
      jsonProtocol.writeI64(i)
    }
  }

  override def writeDouble(v: Double): Unit = {
    if (nestingLevel == 1) {
      values.append(v)
    } else {
      jsonProtocol.writeDouble(v)
    }
  }

  override def writeBinary(byteBuffer: ByteBuffer): Unit = {
    if (nestingLevel == 1) {
      values.append(byteBuffer)
    } else {
      jsonProtocol.writeBinary(byteBuffer)
    }
  }

  override def toString(): String = {
    val sb = new StringBuilder
    values.foreach({x => sb.append(x.toString())})
    return sb.toString()
  }

  override def readBool(): Boolean = ???

  override def readSetBegin(): TSet = ???

  override def readByte(): Byte = ???

  override def readStructBegin(): TStruct = ???

  override def readStructEnd(): Unit = ???

  override def readListEnd(): Unit = ???

  override def readI32(): Int = ???

  override def readI64(): Long = ???

  override def readI16(): Short = ???

  override def readMessageBegin(): TMessage = ???

  override def readFieldBegin(): TField = ???

  override def readListBegin(): TList = ???

  override def readMapEnd(): Unit = ???

  override def readFieldEnd(): Unit = ???

  override def readString(): String = ???

  override def readMessageEnd(): Unit = ???

  override def readDouble(): Double = ???

  override def readBinary(): ByteBuffer = ???

  override def readSetEnd(): Unit = ???

  override def readMapBegin(): TMap = ???
}
