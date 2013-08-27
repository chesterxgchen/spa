package com.xiaoguangchen.spa

trait StatementType

object StatementType extends Enumeration {
  type StatementType = Value
  val CALL,PREPARED = Value
}


object QueryType extends Enumeration {
  type QueryType = Value
  val SelectQuery, UpdateQuery, BatchUpdate = Value
}


import java.sql.Connection

case class QueryInfo(queryType     : QueryType.Value,
                     fetchSize     : Int = 10,
                     isolationLevel: Int = Connection.TRANSACTION_REPEATABLE_READ,
                     statementType : StatementType.Value = StatementType.PREPARED) {

  val batch = queryType match {
    case QueryType.SelectQuery => false
    case QueryType.UpdateQuery => false
    case QueryType.BatchUpdate => true
  }

}

