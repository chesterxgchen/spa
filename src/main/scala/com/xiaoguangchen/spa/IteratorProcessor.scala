package com.xiaoguangchen.spa

import java.sql.ResultSet

/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/17/13 9:35 PM
  */
class IteratorProcessor[A](rs: ResultSet,rowExtractor:RowExtractor[A])
  extends ResultSetProcessor {
  val colMetadataList = readResultSetMetadata(rs)

  def toIterator: Iterator[Option[A]] = new Iterator[Option[A]] {

    //fixme: can we avoid var
    var hasNextState = true
    def next(): Option[A] = {
      if (rs.next()) {
        hasNextState = true
        val rowResult = processRow(rs, colMetadataList, rowExtractor)
        if (rowResult == null) None else Some(rowResult)
      }
      else {
        hasNextState = false
        None
      }

    }

    def hasNext: Boolean = {
       if (hasNextState) {
         hasNextState = rs.next()
         rs.previous() //move the cursor back
       }
      hasNextState
    }
  }


}
