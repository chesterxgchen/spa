package com.xiaoguangchen.spa

import java.sql.ResultSet

/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/17/13 9:35 PM
  */
class IteratorProcessor[A](rs: ResultSet,rowExtractor:RowExtractor[A])
  extends ResultSetProcessor {

  def toIterator: QueryIterator[A] = new QueryIterator[A] {
    val colMetadataList = readResultSetMetadata(rs)
    def next(): Option[A] = {
      if (rs.next()) {
        val rowResult = processRow(rs, colMetadataList, rowExtractor)
        Some(rowResult)
      }
      else
        None
    }
  }



}
