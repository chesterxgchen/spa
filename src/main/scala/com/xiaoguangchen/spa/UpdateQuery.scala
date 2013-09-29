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


          // Note: for MySQL, the returned the column is "GENERATED_KEY" instead of the original specified
          // auto_increment key. And the corresponding     rs.getMetaData.isAutoIncrement(1) is always return false
          // incorrectly

          // For Postgres SQL
          // it will append RETURNING to the origin SQL if the  "Statement.RETURN_GENERATED_KEYS" is used regardless the select, create delete or update
          // Postgres SQL doesn't has good support for returning generated keys
          // If we specified RETURN_GENERATED_KEYS, the Posgres will return all the inserted columns and values in the generatedKeys resultset
          // luckily, rs.getMetaData.isAutoIncrement() does work, and corresponding get column name is also correct.


          getDatabase(connection) match  {
            case MySQL =>
              if (rs.next()) rs.getLong(1) else code.toLong
            case _ =>
              val keys = for {
                i <- 1 to rs.getMetaData.getColumnCount
                if  rs.next() && rs.getMetaData.isAutoIncrement(i)
              } yield rs.getLong(i)
              if (keys.isEmpty) code.toLong else keys.head
          }

        }


      }

    transaction match {
      case None => queryManager.transaction() { trans => innerUpdate(trans.get)}
      case Some(trans) => innerUpdate(trans)
    }
  }


}