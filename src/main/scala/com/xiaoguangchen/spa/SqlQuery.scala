package com.xiaoguangchen.spa

import com.xiaoguangchen.spa.QueryType._
import scala.Predef._
import scala.AnyRef
import java.util.{Calendar, Date}
import java.sql._
import collection._
import java.util.regex.Pattern
import collection.mutable.ArrayBuffer
import scala.Array
import scala.Some
import scala.Some


class SqlQuery[A](val queryManager: QueryManager,
                  val sql: String,
                  queryType: QueryType = QueryType.PREPARED,
                  rowProcessor: RowExtractor[A],
                  transaction : Transaction ) extends Query[A] {

  class ParsedSql(val sql: String, val parameterPositions: mutable.Map[String, ArrayBuffer[Int]])

  private final val SQL_ARG_REGEXP_STR = "(:)(\\w+)"

  val m_parsed = parseSqlString(sql)
  val m_rowProcessor = rowProcessor
  var m_batch = false

  /* JDBC default value */
  var m_fetchSize: Int = 10
  var m_isolationLevel: Int = Connection.TRANSACTION_REPEATABLE_READ


  /** log SQL before query */
  var m_logSql: Boolean = false
  var m_namedParameters: mutable.Map[String, SQLParameter] = mutable.Map()
  var m_positionedParameters: mutable.Map[Int, SQLParameter] = mutable.Map()

  case class PositionParameters(posParam: mutable.Map[Int, SQLParameter] )

  var m_batchPositionedParameters: mutable.ArrayBuffer[PositionParameters] = mutable.ArrayBuffer()

  var m_logPosParameters: mutable.Map[Int, SQLParameter] = mutable.Map()
  var m_logNamedParameters: mutable.Map[String, SQLParameter] = mutable.Map()
  var m_batchLogParameters: mutable.ArrayBuffer[BatchLogParameters] = mutable.ArrayBuffer()


  def isolationLevel(isolation: Int): Query[A] = {

    m_isolationLevel =
      isolation match {
        case Connection.TRANSACTION_READ_COMMITTED |
             Connection.TRANSACTION_READ_UNCOMMITTED |
             Connection.TRANSACTION_REPEATABLE_READ |
             Connection.TRANSACTION_SERIALIZABLE => isolation
        case _ => Connection.TRANSACTION_REPEATABLE_READ
      }
    this
  }

  def fetchSize(fetchSize: Int): Query[A] = {
    m_fetchSize = fetchSize
    this
  }


  def toList(): List[A] = {
    withQuery {  rs =>
      val processor = new SeqResultSetProcessor()
      processor.processResultSet[A](rs, m_rowProcessor).toList
    }
  }

  def toSingle(): Option[A] = {
    val results = toList()
    if (results.isEmpty) None else Some(results.head)
  }

  def withIterator[T]( f: (QueryIterator[A]) => T) : T = {
    withQuery {  rs =>
      val processor = new IteratorProcessor[A](rs, m_rowProcessor)
      f(processor.toIterator)
    }
  }

  def executeUpdate: Int = {
    if (m_batch) throw new QueryException("batch mode should use executeBatchUpdate() method.")

    def internalUpdate(trans: Transaction): Int = {
      setTransactionIsolation(trans.connection)
      val stmt = prepareStatement(trans.connection)
      withStatement(stmt) {
        setParameters(stmt)
        if (m_logSql) logSql()
        stmt.executeUpdate
      }
    }

    withCleanup {
      val trans = this.transaction
      if (trans != null) {
        internalUpdate(trans)
      }
      else {
        queryManager.transaction(this.transaction) { trans =>
          internalUpdate(trans)
        }
      }
    }
  }


  def executeBatchUpdate: Array[Int] = {

   def internalBatchUpdate(trans: Transaction): Array[Int] = {
      setTransactionIsolation(trans.connection)
      val stmt = prepareStatement(trans.connection)
      withStatement(stmt) {
        if (m_logSql) logSql()
        for ( p <- m_batchPositionedParameters) {
          setParameters(stmt, p.posParam)
          stmt.addBatch()
        }
        stmt.executeBatch()
      }
    }

    withCleanup {
      val trans = this.transaction
      if (trans != null) {
        internalBatchUpdate(trans)
      }
      else {
        queryManager.transaction(this.transaction) { trans =>
          internalBatchUpdate(trans)
        }
      }
    }

  }


  def parameterByPos[T](pos: Int, param: T ): Query[A] = {
    checkParameterIndex(pos)
    val sqlParam = getSqlParameter(param)
    setParameterByPos(pos, sqlParam)
    this
  }


  def parameterByName[T](name: String, param: T): Query[A] = {
    val positions = m_parsed.parameterPositions(name)
    require(positions != None, "parameter:" + name + " is not specified in sql statement.")
    val sqlParam = getSqlParameter(param)
    for (pos <- positions; if (pos > 0)) {
      setParameterByPos(pos, sqlParam)
    }

    //used for logging
    m_namedParameters += (name -> sqlParam)

    this
  }


  def addBatch(): Query[A] = {
    m_batch = true
    val posParams = PositionParameters(m_positionedParameters)
    m_batchPositionedParameters  += posParams.copy()

    this
  }

  def logSql(log :Boolean) :Query[A] = {
    m_logSql = log
    this
  }


  ////////////////////////////////////////////////////////////////////

  private def setParameterByPos[T](pos: Int, sqlParam : SQLParameter): Query[A] = {
    checkParameterIndex(pos)
    m_positionedParameters +=(pos->sqlParam)
    this
  }

  private def getSqlParameter[T] (param: T ): SQLParameter = {

    val sqlParam:SQLParameter =
      if (param == null)
        new SQLParameter(param.asInstanceOf[AnyRef],java.sql.Types.NULL)
      else {
        param match {
          case t:Timestamp => new SQLParameter(t,java.sql.Types.TIMESTAMP)
          case d:Date =>      new SQLParameter(new Timestamp(d.getTime),java.sql.Types.TIMESTAMP)
          case c:Calendar =>  new SQLParameter(new Timestamp(c.getTime.getTime),java.sql.Types.TIMESTAMP)

          case s: AnyRef=>    new SQLParameter(s.asInstanceOf[AnyRef],java.sql.Types.JAVA_OBJECT)
          case _ => throw new IllegalArgumentException(" unable to handle parameter " + param )
        }
      }

    sqlParam
  }

  private def withQuery[T] ( f: (ResultSet) =>T) : T = {

    withCleanup {
      queryManager.transaction(this.transaction) { tran =>
        val connection = tran.connection
        val stmt = prepareStatement(connection)
        setTransactionIsolation(connection)
        withStatement(stmt) {
          setParameters(stmt)
          if (m_logSql) logSql()
          stmt.setFetchSize(m_fetchSize)
          val rs = stmt.executeQuery
          withResultSet(rs) {
            f (rs)
          }
        }
      }
    }
  }

  private def withCleanup[T](f: => T): T = {
    try {
      f
    }
    finally {
      cleanup()
    }
  }

  private def cleanup() {
    resetParameterMap()
    resetLogParameterMap()
  }


  private def parseSqlString(sqlString: String): ParsedSql = {
    val sqlBuffer = new StringBuffer()
    val matcher = Pattern.compile(SQL_ARG_REGEXP_STR).matcher(sqlString)
    var pos = 1
    var paramPositions = mutable.Map[String, ArrayBuffer[Int]]()
    while (matcher.find) {
      val arg = matcher.group(2)
      if (arg != null && !arg.isEmpty) {
        matcher.appendReplacement(sqlBuffer, "?")
        var positions = paramPositions.getOrElse(arg, ArrayBuffer[Int]())
        positions += pos
        paramPositions += (arg -> positions)
        pos = pos + 1
      }
    }
    matcher.appendTail(sqlBuffer)
    new ParsedSql(sqlBuffer.toString, paramPositions)
  }

  private def checkParameterIndex(i: Int) {
    if (i <= 0) {
      require(i > 0, "index must start at 1, current index:" + i)
    }
  }

  private def logSql():Unit= {

    println("\n======================================================\n ")
    println("original sql:" + sql + "\n")
    println("parsed sql :" + m_parsed.sql + "\n")
   if (!m_batch) {
      logOneSetParameters(m_logPosParameters, m_logNamedParameters)
    }
    else {
      var batchCount: Int = 1
      for (b <- m_batchLogParameters) {
        println("\n batch  :" + batchCount + "\n")
        logOneSetParameters(b.pos, b.named)
        batchCount += 1
      }
    }
    println("\n======================================================\n ")
  }

  private def logOneSetParameters(posParams: mutable.Map[Int, SQLParameter],
                                  namedParams: mutable.Map[String, SQLParameter]) {
    this.queryType match {
      case PREPARED =>
        for ((i, value) <- posParams; if (posParams != null)) {
          println("parameter " + i + " = " +  value.parameter)
        }
        for ((s, value) <- namedParams; if (namedParams != null)) {
          println("parameter " + s + " = " + value.parameter)
        }
      case CALL => new UnsupportedOperationException(" not implemented")
      case _ => new UnsupportedOperationException(" not implemented")
    }
  }

  private def setParameters(stmt: PreparedStatement, params: mutable.Map[Int, SQLParameter] = m_positionedParameters) {

    copyParametersForLog()
    this.queryType  match {
      case PREPARED =>
        for ((index, p) <- params) {
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

    if (!m_batch) resetParameterMap()

  }

  private def copyParametersForLog() = {

    if (m_logNamedParameters.isEmpty) {
      m_logPosParameters = m_positionedParameters
      m_logNamedParameters = m_namedParameters
    }
    if (m_batch) {
      val pos = m_positionedParameters
      val named = m_namedParameters
      m_batchLogParameters ++ mutable.ListBuffer(new BatchLogParameters(pos, named))
    }

  }

  private def resetParameterMap() {
    m_positionedParameters = mutable.Map()
    m_namedParameters = mutable.Map()
    m_batchPositionedParameters = mutable.ArrayBuffer()

  }

  private def resetLogParameterMap() {
    m_logPosParameters = mutable.Map()
    m_logNamedParameters = mutable.Map()
    m_batchLogParameters = mutable.ArrayBuffer()
  }


  private def prepareStatement(connection: Connection): PreparedStatement = {

    createPrepareStatement {
      parsedSql =>
        val pstmt =  this.queryType  match {
          case PREPARED => {
            connection.prepareStatement(parsedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          }

          // not fully tested
          case CALL => connection.prepareCall(parsedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
          case unknownType => throw new Error("not supported quert type" + unknownType)
        }
        pstmt
    }
  }

  private def createPrepareStatement(f: String => PreparedStatement): PreparedStatement = {
    try {
      val sql = m_parsed.sql
      f(sql)
    }
    catch {
      case e: Throwable => {
        logSql()
        throw new PreparedStatementException(String.format("failed to prepare statement: %s", sql), e)
      }
    }
  }


  private def closeStatement(stmt: Statement) {
    try {if (stmt != null) stmt.close()} catch {case _: Throwable =>}
  }


  private def withStatement[T](stmt: Statement)(f: => T): T = {
    try {
      f
    }
    catch {
      case e: Throwable => {logSql(); throw new PreparedStatementException("failed execute query sql: " + sql, e)}
    }
    finally {
      closeStatement(stmt)
    }
  }


  private def withResultSet[T](rs: ResultSet)(f: => T): T = {
    try {
      f
    }
    catch {
      case e: Throwable => {logSql(); throw new ExecuteQueryException("failed execute query sql" + sql, e)}
    }
    finally {
      try {if (rs != null) rs.close()} catch {case _: Throwable =>}
    }
  }

  private def setTransactionIsolation(connection: Connection) {
    if (m_isolationLevel != connection.getTransactionIsolation)
      connection.setTransactionIsolation(m_isolationLevel)
  }


}