package com.xiaoguangchen.spa

import scala.Predef._
import scala.collection.mutable.ArrayBuffer
import com.xiaoguangchen.spa


/**

 * Design note: Transaction must be provided, the connection can be obtained
 * from the transaction. After the query, the connect is not immediately closed.
 *
 * The connection closure will close the connection
 * after the transaction is committed or roll back; therefore, there is no need for query to close it.
 *
 * This also bring the benefits that within one transaction, multiple queries can share
 * the same transaction/connection, therefore, there is no need to close and re-open the connection for
 * queries with the same transaction and transaction commit/rollback will work for multiple statements
 *
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/17/13 8:25 PM
  */

class BatchUpdateQuery(queryManager  : QueryManager,
                       parsedSql     : ParsedSql)
                      (implicit transaction : Option[Transaction])
                       extends CoreQuery[BatchUpdateQuery](parsedSql,batch=true) {

  case class PositionParameters(posParam: Map[Int, SQLParameter] )

  //todo: using var, see if we can change it,
  var batchPositionedParameters = ArrayBuffer[PositionParameters]()

  def addBatch(params : Map[Int, Any]): BatchUpdateQuery = {
     val sqlParams = params.map(a => a._1 ->spa.toSqlParameter(a._2))
     batchPositionedParameters += PositionParameters(sqlParams)
    this
  }

  def executeBatchUpdate: Array[Int] = {

    def innerUpdate(trans : Transaction) =
      withCleanup {
        val connection = trans.connection
        setTransactionIsolation(connection)
        val stmt = prepareStatement(connection)
        withStatement(stmt) {
          if (logSql) showSql()
          for ( p <- batchPositionedParameters ) {
            setParameters(stmt, p.posParam)
            stmt.addBatch()
          }
          stmt.executeBatch()
        }
      }

    transaction match {
      case None => queryManager.transaction() { trans => innerUpdate(trans)}
      case Some(trans) => innerUpdate(trans)
    }

  }
//////////////////////////////////////////////////
  override private[spa] def cleanup() {
      super.cleanup()
      batchPositionedParameters = ArrayBuffer[PositionParameters]()
  }


  override private[spa] def showSql() = {
    println("\n======================================================\n ")
    println(s" sql: ${parsedSql.sql}")

    println("batch size =" + batchPositionedParameters.size)

    var batchCount: Int = 1
    batchPositionedParameters.foreach { b =>
      println(s"\n batch:$batchCount \n")
      showParameters(b.posParam)
      batchCount += 1
    }

    println("\n======================================================\n ")
  }

}