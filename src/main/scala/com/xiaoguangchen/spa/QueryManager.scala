package com.xiaoguangchen.spa

/**
 * Chester Chen (chesterxgchen@yahoo.com)
 * User: Chester Chen
 * Date: 12/25/12
 * Time: 5:14 AM
 *
 */

import java.sql.{SQLException, Connection}
import scala.Predef._
import com.xiaoguangchen.spa.QueryType._


object QueryManager {

  def apply(open: => Connection, logConnection:Boolean= false): QueryManager = {
    new QueryManager(open, logConnection)
  }

}

class QueryManager (open: => Connection, logConnection:Boolean) {

  def queryWithClass[A](sqlString:   String,
                        resultClass: Class[A],
                        transaction: Transaction = null,
                        queryType:   QueryType = QueryType.PREPARED): Query[A] = {
    query(sqlString, new ClassRowProcessor[A](resultClass), transaction,queryType)
  }

  def queryForUpdate[A](sqlString: String,transaction: Transaction = null, queryType: QueryType = QueryType.PREPARED ): Query[A] = {
    if (transaction == null) {
        query(sqlString, null, transaction,queryType)
    }
    else
      query(sqlString, null, transaction,queryType)

  }


  def query[A](sqlString:   String,
               rowProcessor: RowExtractor[A],
               transaction: Transaction = null,
               queryType:QueryType = QueryType.PREPARED): Query[A] = {
    new SqlQuery[A](this, sqlString, queryType, rowProcessor, transaction)
  }


  def transaction[T](transaction : Transaction = null)(f: Transaction => T):T = {
    withTransaction(transaction) { tran =>
      try {
        tran.begin()
        val value = f(tran)
        tran.commit()
        value
      } catch {
        case e: Throwable =>
          tran.rollback()
          throw e
      }
    }
  }


  /////////////////////////////////////////////////////////////////

  protected[spa]  def withConnection[A](conn : Connection) (f: Connection => A): A = {
    val connection = if(conn == null) open else conn
    if (logConnection && conn == null) println("Database connection established")

    checkConnection(connection)
    try {
      f(connection)
    } finally {
      close(connection)
    }
  }

  private def withTransaction[A](transaction : Transaction) (f: Transaction => A):A = {
    val conn = if (transaction == null) null else transaction.connection
    withConnection(conn) { connection =>
      if (transaction == null)
        f(new Transaction(connection))
      else
        f(transaction)
    }
  }


  private def checkConnection(connection: Connection) {
    require(connection != null && !connection.isClosed)
  }

  private def close(conn:Connection ) {
    try {
      if (conn != null && !conn.isClosed)  {
        conn.close()
        if (logConnection) println("database connection closed!")
      }
    }
    catch {
      case e: SQLException => println("failed to close Connection", e)
      case _ =>
    }
  }



}
