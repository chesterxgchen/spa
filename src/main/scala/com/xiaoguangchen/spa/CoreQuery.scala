package com.xiaoguangchen.spa

import java.sql.{ResultSet, Statement, PreparedStatement, Timestamp, Connection}
import StatementType._
import com.xiaoguangchen.spa.QueryType._


/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 7/14/13
 */


private [spa] abstract class CoreQuery[Q](parsedSql     : ParsedSql ,
                                          queryInfo     : QueryInfo) {



  checkIsolationLevel(queryInfo.isolationLevel)

  /** log SQL before query */

  private[spa] var logSql = false
  def logSql(log: Boolean): Q = {
    this.logSql = log
    this.asInstanceOf[Q]
  }

////////////////////////////////////////////////////////////////////////////
// package private


  private def checkIsolationLevel(isolation: Int): Unit = {

    val level =  isolation match {
      case Connection.TRANSACTION_READ_COMMITTED |
           Connection.TRANSACTION_READ_UNCOMMITTED |
           Connection.TRANSACTION_REPEATABLE_READ |
           Connection.TRANSACTION_SERIALIZABLE => isolation
      case _ => throw new QueryException(s"isolation level $isolation is not known, isolation level must be one defined in java.sql.Connection ")
    }
  }

  private[spa] def withCleanup[T](f: => T): T = {
    try {
      f
    }
    finally {
      cleanup()
    }
  }



  private[spa] def prepareStatement(connection: Connection ): PreparedStatement = {

    val database = getDatabase(connection)
    createPrepareStatement { parsedSql =>

        val pstmt =  queryInfo.statementType  match {
          case PREPARED => {
            queryInfo.queryType match {
             // case QueryType.SelectQuery => connection.prepareStatement(parsedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
              case QueryType.SelectQuery => connection.prepareStatement(parsedSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
              case QueryType.UpdateQuery => {
                database match {
                  case MySQL => connection.prepareStatement(parsedSql, Statement.RETURN_GENERATED_KEYS)
                  case Postgres =>
                    // note: Postgres will append RETURNING to the origin SQL if the  "Statement.RETURN_GENERATED_KEYS" is used regardless the select, create delete or update
                    val sql = parsedSql.trim.toLowerCase
                    if (sql.startsWith("update") )
                       connection.prepareStatement(parsedSql, Statement.RETURN_GENERATED_KEYS)
                    else {
                      connection.prepareStatement(parsedSql, Statement.NO_GENERATED_KEYS)
                    }


                  case OtherDatabase => connection.prepareStatement(parsedSql, Statement.RETURN_GENERATED_KEYS)
                }

              }
              case QueryType.BatchUpdate => connection.prepareStatement(parsedSql, Statement.NO_GENERATED_KEYS)
            }
          }

          case CALL => {
            // not tested
            queryInfo.queryType match {
              case QueryType.SelectQuery => connection.prepareCall(parsedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
              case QueryType.UpdateQuery => connection.prepareCall(parsedSql)
              case QueryType.BatchUpdate => sys.error(" not supported")
            }
          }
          case _ => sys.error("not supported query type" )
        }
        pstmt
    }
  }


  def getDatabase(connection: Connection) :Database = {

    val MySQLUrlPattern = "jdbc:mysql:.*".r
    val PostgresUrlPattern = "jdbc:postgresql:.*".r

    connection.getMetaData.getURL match {
      case MySQLUrlPattern() => MySQL
      case PostgresUrlPattern() => Postgres
      case _ => OtherDatabase
    }

  }

  private[spa] def setTransactionIsolation(connection: Connection) {
    if (queryInfo.isolationLevel != connection.getTransactionIsolation)
      connection.setTransactionIsolation(queryInfo.isolationLevel)
  }


  private[spa] def withStatement[T](stmt: Statement)(f: => T): T = {
    try {
      f
    }
    catch {
      case e: Throwable => {
        showSql()
        throw new PreparedStatementException(s"failed to execute query sql: ${parsedSql.sql}", e)}
    }
    finally {
      closeStatement(stmt)
    }
  }

  //todo: can we removed the second parameter
  private[spa] def setParameters(stmt: PreparedStatement, params: Map[Int, SQLParameter] = parsedSql.parameterPositions) {

    queryInfo.statementType  match {
      case PREPARED =>
        for ((i, p) <- params) {
          val index = i+1
          (p.parameter, p.sqlType) match {
            case (n, java.sql.Types.NULL) => stmt.setNull(index, p.sqlType)
            case (t, java.sql.Types.TIMESTAMP) => stmt.setTimestamp(index, t.asInstanceOf[Timestamp])
            case (d, java.sql.Types.DECIMAL) => stmt.setBigDecimal(index, d.asInstanceOf[java.math.BigDecimal])
            case (x, java.sql.Types.NUMERIC) => stmt.setBigDecimal(index, x.asInstanceOf[java.math.BigDecimal])
            case (o, java.sql.Types.JAVA_OBJECT) =>  stmt.setObject(index, o)
            case _ => throw new IllegalArgumentException(" unable to handle parameter " + p )
          }
        }
      case CALL => new Error("not implemented")
      case _ => new Error("not implemented")
    }


  }

  private[spa] def showSql() = {

    println("\n======================================================\n ")
    println(s"sql: ${parsedSql.sql} \n")
    showParameters(parsedSql.parameterPositions)
    println("\n======================================================\n ")
  }

  private[spa] def cleanup() {}

  private[spa] def showParameters(params: Map[Int, SQLParameter]) {
    queryInfo.statementType match {
      case PREPARED =>
        for ((a, value) <- params) println(s"parameter ${a+1}=${value.parameter}")

      case CALL => new UnsupportedOperationException(" not implemented")
      case _ => new UnsupportedOperationException(" not implemented")
    }
  }


  ///////////////////////////////////////////////////////////////////////////
  //private

  private def createPrepareStatement(f: String => PreparedStatement): PreparedStatement = {
    try {
      f(parsedSql.sql)
    }
    catch {
      case e: Throwable => {
        showSql()
        throw new PreparedStatementException(s"failed to prepare statement: ${parsedSql.sql}", e)
      }
    }
  }

  private def closeStatement(stmt: Statement) {
    try {if (stmt != null) stmt.close()} catch {case _: Throwable =>}
  }




}
