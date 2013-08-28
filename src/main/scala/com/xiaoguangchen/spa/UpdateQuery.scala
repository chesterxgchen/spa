package com.xiaoguangchen.spa

import java.sql.{ResultSetMetaData, Connection, Statement}

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
                  parsedSql     : ParsedSql,
                  level: Int = Connection.TRANSACTION_REPEATABLE_READ)
                  (transaction   : Option[Transaction])
                  extends CoreQuery[UpdateQuery](parsedSql, QueryInfo(QueryType.UpdateQuery, isolationLevel = level)) {

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