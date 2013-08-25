package com.xiaoguangchen.spa

import java.sql.{DriverManager, SQLException, Connection}

/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 7/14/13

 */

object QueryManager {


  def getConnection(driverName: String, url: String, userName:String, password:String): Option[Connection] = {
    try {
      Class.forName(driverName).newInstance()
      val conn = DriverManager.getConnection(url, userName, password)
      Some(conn)
    }
    catch {
      case ex:Throwable => {
        ex.printStackTrace()
        None
      }
    }
  }


  def apply(open: => Option[Connection], logConnection:Boolean= false): QueryManager = {
    new QueryManager(open, logConnection)
  }

}


class QueryManager (open: => Option[Connection], logConnection:Boolean) {

  def selectQuery(parsedSql:ParsedSql,
                  rowProcessor: Option[RowExtractor[_]] = None)
                 (implicit trans : Option[Transaction] = None): SelectQuery = {
       new SelectQuery(this, parsedSql)(rowProcessor)(trans)
  }


  def updateQuery(parsedSql:ParsedSql)
                 (implicit trans : Option[Transaction] = None): UpdateQuery = {
    new UpdateQuery(this, parsedSql)(trans)
  }

  def batchUpdateQuery(parsedSql:ParsedSql)
                       (implicit trans : Option[Transaction] = None): BatchUpdateQuery = {
    new BatchUpdateQuery(this, parsedSql)(trans)
  }




  def transaction[T](transaction : Option[Transaction] = None)(f: Transaction => T):T = {
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

  protected[spa]  def withConnection[A](conn : Option[Connection]) (f: Option[Connection] => A): A = {
    val connection = if(conn.isDefined) conn else open
    if (logConnection && conn.isEmpty) println("Database connection established")

    checkConnection(connection)
    try {
      f(connection)
    } finally {
      close(connection)
    }
  }

  private def withTransaction[A](transaction : Option[Transaction]) (f: Transaction => A):A = {
    val conn = if (transaction.isDefined) Some(transaction.get.connection) else None

    withConnection(conn) { connection: Option[Connection] =>
      val trans = transaction.getOrElse(new Transaction(connection.get))
      f(trans)
    }
  }


  private def checkConnection(connection: Option[Connection]) {
    require(connection.isDefined)
    require(!connection.get.isClosed)
  }

  private def close(conn:Option[Connection] ) {
    try {
      if (conn.isDefined && !conn.get.isClosed)  {
        conn.get.close()
        if (logConnection) println("database connection closed!")
      }
    }
    catch {
      case e: SQLException => println("failed to close Connection", e)
      case _:Throwable =>
    }
  }



}
