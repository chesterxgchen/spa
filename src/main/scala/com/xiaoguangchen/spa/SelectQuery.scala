package com.xiaoguangchen.spa

import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import java.sql.{Connection, ResultSet}
import scala.Some

/**
 * Select Queries:
 *
 * Design note: Transaction must be provided, the connection can be obtained
 * from the transaction. After the query, the connect is not immediately closed,
 * the connection will be closed by transaction closure via connection closure.
 *
 * The connection closure will close the connection
 * after the transaction is committed or roll back; therefore, there is no need for query to close it.
 *
 * This also bring the benefits that within one transaction, multiple queries can share
 * the same transaction/connection, therefore, there is no need to close and re-open the connection for
 * queries with the same transaction and transaction commit/rollback will work for multiple statements
 *
 * For select query only, we might not need a transaction, but when we have mix of select and update query, for example
 *
 *   select
 *   update
 *   select
 *   delete
 *   select
 *   update
 *
 *   We don't want to start a new connection and close the new connection for every query (select or update), we would like them
 *   all use the same connection under one transaction. Therefore, even during select query we are also using a transaction.
 *
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 7/14/13
 */



class SelectQuery(queryManager  : QueryManager,
                  parsedSql     : ParsedSql,
                  fetchSize     : Int = 10,
                  isolationLevel: Int = Connection.TRANSACTION_REPEATABLE_READ)
                 (rowProcessor: Option[RowExtractor[_]] = None)
                 (transaction : Option[Transaction] = None)
                  extends CoreQuery[SelectQuery](parsedSql, fetchSize, isolationLevel) {

  def toList[A : ClassTag : TypeTag]: List[A] = {

    def innerToList (trans: Transaction)=
      withQuery (trans) {  rs =>
        val processor = new SeqResultSetProcessor()
        val extractor = rowProcessor.getOrElse( new ClassRowProcessor[A])
          .asInstanceOf[RowExtractor[A]]
        processor.processResultSet[A](rs, extractor).toList
      }

    transaction match {
       case None => queryManager.transaction() { trans => innerToList(trans)}
       case Some(trans) => innerToList(trans)
     }


  }

  def toSingle[A : ClassTag : TypeTag]: Option[A] = {
    val results = toList[A]
    if (results.isEmpty) None else Some(results.head)
  }



  def withIterator[T, A : ClassTag : TypeTag]( f: (Iterator[Option[A]]) => T)  : T = {

    val extractor = rowProcessor.getOrElse( new ClassRowProcessor[A] ).asInstanceOf[RowExtractor[A]]

    def innerWithIterator(trans: Transaction) =
      withQuery(trans) {  rs =>
        val processor = new IteratorProcessor[A](rs, extractor)
        f(processor.toIterator2)
      }

    transaction match {
      case None =>
        queryManager.transaction() { trans =>
          innerWithIterator(trans)
        }
      case Some(trans) => innerWithIterator(trans)
    }

  }




  ///////////////////////////////////////////////////////////////////


  private def withQuery[T](trans: Transaction) ( f: (ResultSet) =>T) : T = {
    withCleanup {
        val connection = trans.connection
        val stmt = prepareStatement(connection)
        setTransactionIsolation(connection)
        withStatement(stmt) {
          if (this.logSql) showSql()
          setParameters(stmt)
          stmt.setFetchSize(fetchSize)
          val rs = stmt.executeQuery
          withResultSet(rs) {
            f (rs)
          }
        }

    }
  }

  private def withResultSet[T](rs: ResultSet)(f: => T): T = {
    try {
      f
    }
    catch {
      case e: Throwable => {
        showSql()
        throw new ExecuteQueryException("failed execute query sql" + parsedSql.sql, e)
      }
    }
    finally {
      try {if (rs != null) rs.close()} catch {case _: Throwable =>}
    }
  }


}
