package com.xiaoguangchen.spa

import java.sql.{Timestamp, ResultSet}
import collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala._
import scala.Predef._
import scala.AnyRef
import scala.Any
import java.io.{IOException, ObjectInputStream, InputStream}
import java.util.Date

/*
 * Copyright 2011 Chester Chen (Xiaoguang Chen chesterxgchen@yahoo.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
class SeqResultSetProcessor extends ResultSetProcessor {


  def processResultSet[T](rs: ResultSet,rowExtractor:RowExtractor[T]) : Seq[T] = {
    val colMetadataList = readResultSetMetadata(rs)
    var resultList = ArrayBuffer[T]()

    while (rs.next) {
      val rowResult = processRow(rs, colMetadataList, rowExtractor)
      resultList += rowResult
    }
    resultList
  }
}

