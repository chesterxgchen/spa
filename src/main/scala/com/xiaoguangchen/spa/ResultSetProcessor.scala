package com.xiaoguangchen.spa

import java.sql.{Timestamp, ResultSet}
import scala.collection.mutable.ArrayBuffer
import scala._
import collection.mutable
import java.util.Date
import java.io.{ByteArrayInputStream, IOException, ObjectInputStream, InputStream}


/**
 * Chester Chen (chesterxgchen@yahoo.com)
 * User: Chester
 * Date: 12/23/12
 * Time: 4:39 PM
 */


trait ResultSetProcessor {

  protected def readResultSetMetadata(resultSet: ResultSet): IndexedSeq[ColumnMetadata] = {
    val rsmd = resultSet.getMetaData
    val columnCount = rsmd.getColumnCount
    for (i <- 0 until columnCount)
         yield new ColumnMetadata(i, rsmd.getColumnType(i + 1),
            rsmd.getColumnName(i + 1),
            rsmd.getColumnLabel(i + 1),
            rsmd.getScale(i + 1),
            rsmd.getPrecision(i + 1) )
  }

  protected def processRow[T](rs: ResultSet, md: IndexedSeq[ColumnMetadata], rowExtractor: RowExtractor[T]): T = {
    val rowValues = toRowMap(rs, md)
    val result = rowExtractor.extractRow(rowValues)
    result
  }


  private def timestampToUtilDate(timestamp: Timestamp): Date = {
    if (timestamp != null) new Date(timestamp.getTime) else null
  }

  private def getValue(rs: ResultSet, cmd: ColumnMetadata, columnOffset: Int): Any = {

    if (rs.wasNull) return null
    val value = cmd.colType match {
      case java.sql.Types.INTEGER | java.sql.Types.SMALLINT | java.sql.Types.TINYINT => rs.getInt(columnOffset)
      case java.sql.Types.BIGINT => rs.getLong(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale == 0 && cmd.colPrecision == 0 => rs.getDouble(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale <= 0 && cmd.colPrecision <= 9 => rs.getInt(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale <= 0 && cmd.colPrecision > 9 && cmd.colPrecision <= 18 => rs.getLong(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale <= 0 && cmd.colPrecision > 18 => rs.getBigDecimal(columnOffset)
      case java.sql.Types.DOUBLE | java.sql.Types.FLOAT =>  rs.getDouble(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale > 0 && cmd.colPrecision > 9 => rs.getBigDecimal(columnOffset)
      case java.sql.Types.DECIMAL | java.sql.Types.NUMERIC if cmd.colScale > 0 && cmd.colPrecision <= 9 => rs.getDouble(columnOffset)
      case java.sql.Types.DATE | java.sql.Types.TIME | java.sql.Types.TIMESTAMP => timestampToUtilDate(rs.getTimestamp(columnOffset))
      case java.sql.Types.VARCHAR => rs.getString(columnOffset)
      case java.sql.Types.BLOB => rs.getBlob(columnOffset)
      case java.sql.Types.CLOB => rs.getString(columnOffset)
      case java.sql.Types.JAVA_OBJECT => rs.getObject(columnOffset)
    //  case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY => objectValueFromBytes(rs, columnOffset)
    //  case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY => getBytes(rs.getBlob(columnOffset))
    //    case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY => objectValueFromBytes2(rs, columnOffset)
      case java.sql.Types.BINARY | java.sql.Types.VARBINARY | java.sql.Types.LONGVARBINARY => rs.getBlob(columnOffset)
      case _ => rs.getObject(columnOffset)
    }

    value

  }

  private def toRowMap(rs: ResultSet, md: IndexedSeq[ColumnMetadata]): Map[ColumnMetadata, Any] = {
    val columnCount = md.size
    var resultMap = mutable.Map[ColumnMetadata, Any]()

    for (i <- 0 until columnCount; cmd = md(i)) {
      resultMap += (cmd -> getValue(rs, cmd, i + 1))
    }

    resultMap.toMap
  }


  private def getBytes(blob: java.sql.Blob) = {
  val objectIn = new ObjectInputStream(blob.getBinaryStream)
      objectIn.readObject
  }


  private def objectValueFromBytes(rs: ResultSet, columnOffset: Int): Any = {
    try {
      val value =  if (!rs.wasNull) {
        val is: InputStream = rs.getBinaryStream(columnOffset)
        if (is != null) {
          val objectIn: ObjectInputStream = new ObjectInputStream(is)
          objectIn.readObject
        }
      } else null

      value
    }
    catch {
      case e: IOException => {
        throw new QueryException("read binary stream failed at column" + columnOffset + ", make sure database connection is not closed", e)
      }
      case e: ClassNotFoundException => {
        throw new QueryException("read binary stream failed at column" + columnOffset + ", class not found", e)
      }
    }
  }


  private def objectValueFromBytes2(rs: ResultSet, columnOffset: Int): Any = {
    try {
      val value =  if (!rs.wasNull) {
        rs.getBytes(columnOffset)
      } else null

      value
    }
    catch {
      case e: IOException => {
        throw new QueryException("read binary stream failed at column" + columnOffset + ", make sure database connection is not closed", e)
      }
      case e: ClassNotFoundException => {
        throw new QueryException("read binary stream failed at column" + columnOffset + ", class not found", e)
      }
    }
  }


}
