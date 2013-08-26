package com.xiaoguangchen.spa

import scala.Predef._
import scala.Array
import scala.reflect.ClassTag

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

class UpdateQuery(queryManager  : QueryManager,
                  parsedSql     : ParsedSql)
                 (transaction   : Option[Transaction])
                  extends CoreQuery[UpdateQuery](parsedSql) {

  def executeUpdate: Long = {

    def innerUpdate(trans : Transaction) =
      withCleanup {
        val connection = trans.connection
        setTransactionIsolation(connection)
        val stmt = prepareStatement(connection)
        withStatement(stmt) {
          setParameters(stmt)
          if (logSql) showSql()
          val code = stmt.executeUpdate

          val rs = stmt.getGeneratedKeys
          if (rs.next()) rs.getLong(1) else code.toLong
        }
      }

    transaction match {
      case None => queryManager.transaction() { trans => innerUpdate(trans.get)}
      case Some(trans) => innerUpdate(trans)
    }
  }


}