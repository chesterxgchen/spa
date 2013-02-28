
package com.xiaoguangchen.spa

import annotation.Column
import java.sql.{DriverManager, Connection}
import org.testng.annotations.Test
import reflect.BeanProperty
import java.util.Date
import collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.math.{MathContext, BigDecimal}
import runtime.ScalaRunTime
import math.BigDecimal.RoundingMode


/**
 * Chester Chen (chesterxgchen@yahoo.com
 * Date: 6/3/12
 * Time: 10:13 PM
 *
 * NOTE: The tests require to have MYSQL installed.
 * You will also need to configure the application.properties to provide the root password
 * for the mySQL database
 *
 */

/**
 * Several tests will be query:
 * select table_schema,table_name,table_type,create_time,ifNull(checksum,-1) as 'checksum'
 * from information_schema.tables limit 10.
 *
 * The results from mySQL is the following
 *
+--------------------+---------------------------------------+-------------+---------------------+----------+
| schema             | name                                  | tableType   | createTime          | checksum |
+--------------------+---------------------------------------+-------------+---------------------+----------+
| information_schema | CHARACTER_SETS                        | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | COLLATIONS                            | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | COLLATION_CHARACTER_SET_APPLICABILITY | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | COLUMNS                               | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | COLUMN_PRIVILEGES                     | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | ENGINES                               | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | EVENTS                                | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | FILES                                 | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | GLOBAL_STATUS                         | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
| information_schema | GLOBAL_VARIABLES                      | SYSTEM VIEW | 2013-01-19 16:47:41 |       -1 |
+--------------------+---------------------------------------+-------------+---------------------+----------+
 *
 *
 */
object QueryTest {

  class TableMetadata () {

    @BeanProperty var schema:String = null
    @BeanProperty var name:String = null
    @BeanProperty var tableType:String = null
    @BeanProperty var createTime: Date = null
    @BeanProperty var checksum: Long = -1

    override def toString: String = (schema, name, tableType, createTime, checksum).toString()
  }

  class TableMetadata2 () {

    @Column("table_schema") var schema:String = null
    @Column("table_name")  var name:String = null
    @Column("table_type")  var tableType:String = null
    @Column("create_time")  var createTime: Date = null
    @Column("checksum")  var checksum: Long = -1

    override def toString: String = (schema, name, tableType, createTime, checksum).toString()
  }


  class TableMetadata4 () {

    var schema:String = null
    @Column("table_schema")
    def setSchema(s : String) = this.schema = s

    var name:String = null
    @Column("table_name")
    def setName(s: String)  = this.name = s

    @Column("table_type") var tableType:String = null
    @Column("create_time")  var createTime: Date = null
    @Column("checksum")  var checksum: Long = -1

    override def toString: String = (schema, name, tableType, createTime, checksum).toString()
  }

  class TableMetadata5 (@Column("table_schema") val schema:String,
                        @Column("table_name") val name:String,
                        @Column("table_type") val tableType: String,
                        @Column("create_time") val createTime: Date = null,
                        @Column("checksum") val checksum: Long = -1)    {

    override def toString: String = (schema, name, tableType, createTime, checksum).toString()
  }



  class TableMetadata6 (@Column("table_schema") val schema:String,
                        @Column("table_name") val name:String,
                        @Column("table_type") val tableType: String)    {

    @Column("create_time") var createTime: Date = null
    @Column("checksum")   var  checksum: Long = -1

    override def toString: String = (schema, name, tableType, createTime, checksum).toString()
  }


  class SerializedObj(x: String, y: Date, z:Long) extends Serializable {
    override def toString: String = "(" + x + "," + z + ")"
  }



}

class QueryTest extends BaseTest {

 @Test(groups = Array("select", "query", "single"))
 def testSelectSingle() {
   val qm = QueryManager(open = getConnection )
   val sqlString: String = "select table_name from information_schema.tables WHERE TABLE_NAME = ?"
   val result = qm.queryWithClass(sqlString, classOf[String])
     .parameterByPos(1, "Tables")
     .toSingle()
   assert(result != None)
   assert(result.get.equalsIgnoreCase("TABLES"))
 }

  @Test(groups = Array("select", "query", "single"))
  def testSelectSingleValue() {
    val qm = QueryManager(open = getConnection )
    val sqlString = "select count(*) from information_schema.tables "
    val result = qm.queryWithClass(sqlString, classOf[Long]).toSingle()
    assert(result != None)
    assert(result.get > 0)
  }

  @Test(groups = Array("select", "query", "named-arg"))
 def testNamedArg() {
    val qm = QueryManager(open = getConnection)
    val sqlString: String = "select table_name from information_schema.tables WHERE table_name = :tableName"
    val result = qm.queryWithClass(sqlString, classOf[String])
      .parameterByName("tableName", "Tables")
      .toSingle()
   assert(result != None)
   assert(result.get.equalsIgnoreCase("TABLES"))
  }

  @Test(groups = Array("select", "query", "native-query"))
  def testSelectList() {
    val qm = QueryManager(open = getConnection)
    val results = qm.queryWithClass("show databases", classOf[String]).toList()
    assert (!results.isEmpty)

    assert(!results.filter(_.toLowerCase == "information_schema").isEmpty)
    assert(!results.filter(_.toLowerCase == "mysql").isEmpty)
    assert(!results.filter(_.toLowerCase == "performance_schema").isEmpty)

  }

  @Test(groups = Array("select", "query", "jdbc"))
  def testFetchSize() {
    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_name from information_schema.tables limit 10, 20"
    val results = qm.queryWithClass(sqlString, classOf[String]).fetchSize(5).toList()
    assert (!results.isEmpty)
  }



  @Test(groups = Array("select", "query"))
  def testSelectWithBeanProperties() {


    val qm = QueryManager(open = getConnection)
    val sqlString = " select table_schema as 'schema' , " +
                    "        table_name as 'name', " +
                    "        table_type as 'tableType', " +
                    "        create_time as 'createTime', " +
                    "        ifNull(checksum, -1)  as 'checksum' " +
                    " from information_schema.tables limit 10 "

    // use setter method with BeanProperties, no column annotation
    // for example, the BeanProperty annotation for var schema will generate a setSchema setter method
    // then the corresponding column label should be "schema"; If the column name is "table_schema" then we need to use table_schema as 'schema'
    // to make column table as "schema"

    val results = qm.queryWithClass(sqlString,classOf[QueryTest.TableMetadata]).toList()
    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }

  @Test(groups = Array("select", "query"))
  def testSelectWithFieldColumnAnn() {

    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
                    " from information_schema.tables limit 10"
    // use Column Annotation on the var fields
    val results = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata2] ).toList()
    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }

  @Test(groups = Array("select", "query"))
  def testSelectWithSetterAndFieldColumnAnn() {

    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
      " from information_schema.tables limit 10"
    // use Column Annotation on the var fields  and some setter methods
    val results = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata4] ).toList()
    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }


  @Test(groups = Array("select", "query"))
  def testSelectWithConstructorColumnAnn() {

    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
      " from information_schema.tables limit 10"
    // use Column Annotation on the parameters of the constructor
    val results = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata5] ).toList()

    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }

  @Test(groups = Array("select", "query"))
  def testSelectWithMixFieldAndConstructorColumnAnn() {

    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
      " from information_schema.tables limit 10"
    // use Column Annotation on the parameters of the constructor
    val results = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata6] ).toList()

    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }

  @Test(groups = Array("select", "query", "iterator"))
  def testSelectWithIterator() {

    val qm = QueryManager(open = getConnection)
    val sqlString = "select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
      " from information_schema.tables limit 10"
    // use Column Annotation on the parameters of the constructor
    val q = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata6] )

    var results = ArrayBuffer[QueryTest.TableMetadata6]()
    q.withIterator { it =>
      var r = it.next()
      while (r != None) {
        results  += r.get
        r = it.next()
      }
    }


    assert (results.size == 10)
    assert (results.filter(c => c.name.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c.name.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c.tableType.toUpperCase == "SYSTEM VIEW").size == 10)
    assert (results.filter(c => c.schema.toLowerCase == "information_schema").size == 10)
    assert (results.filter(c => c.checksum == -1).size == 10)
  }


  @Test(groups = Array("update", "query"))
  def testUpdate() {
    val qm = QueryManager(open = getConnection)
    val dropDbSql = "drop database if exists mytest"
    qm.queryForUpdate(dropDbSql).executeUpdate
    val createDbSql = "create database if not exists mytest"
    qm.queryForUpdate(createDbSql).executeUpdate

    val selectTableSql: String = " select count(*) from information_schema.tables " +
      " where table_schema = :db and table_name = :table "

    var count = qm.queryWithClass(selectTableSql, classOf[Long] )
                  .parameterByName("db", "mytest")
                  .parameterByName("table", "test")
                  .toSingle()
    assert(count.get == 0)

    val createTableSql = "create table if not exists mytest.test(x Integer)"
    qm.queryForUpdate(createTableSql).executeUpdate

    count = qm.queryWithClass(selectTableSql, classOf[Long] )
              .parameterByName("db", "mytest")
              .parameterByName("table", "test")
              .toSingle()
    assert(count.get >= 1)
  }

  @Test(groups = Array("update", "query", "batchUpdate"))
  def testBatchUpdate() {
    val qm = QueryManager(open = getConnection)
    val dropDbSql = "drop database if exists mytest"
    qm.queryForUpdate(dropDbSql).executeUpdate
    val createDbSql = "create database if not exists mytest"
    qm.queryForUpdate(createDbSql).executeUpdate

    val createTableSql = "create table if not exists mytest.test(x Integer)"
    qm.queryForUpdate(createTableSql).executeUpdate


    val InsertSql = "insert into mytest.test(x) values (:x)"
    qm.transaction() { trans =>
      assert(trans.connection != null)
      assert(!trans.connection.isClosed)

      val q = qm.queryForUpdate(InsertSql, trans)
      for (i <- 1 to 200 ) {
        q.parameterByName("x", i)
        q.addBatch()
      }
      q.executeBatchUpdate
    }

    val count = qm.queryWithClass( "select count(*) from mytest.test", classOf[Long]).toSingle()
    assert(count != None && count.get == 200 )
  }

  @Test(groups = Array("update", "query", "transaction"))
  def testUpdateTransaction() {
    val qm = QueryManager(open = getConnection, logConnection = true)
    val dropDbSql = "drop database if exists mytest"
    qm.queryForUpdate(dropDbSql).executeUpdate
    val createDbSql = "create database if not exists mytest"
    qm.queryForUpdate(createDbSql).executeUpdate

    val createTableSql = "create table if not exists mytest.test(x Integer)"
    qm.queryForUpdate(createTableSql).executeUpdate

    try {
      qm.transaction() {transaction =>
          qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
            .parameterByName("x", 1).executeUpdate

          qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
            .parameterByName("x", 2).executeUpdate
          throw new Error(" let it fail during transaction")
      }
    }
    catch {
      case _:Error =>
    }

    //the inserted rows should have rollback.
    var count = qm.queryWithClass( "select count(*) from mytest.test", classOf[Long]).toSingle()
    assert(count != None && count.get == 0 )

    //do one more times
    qm.transaction() { transaction =>
      qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
        .parameterByName("x", 1).executeUpdate

      qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
        .parameterByName("x", 2).executeUpdate
    }
    count = qm.queryWithClass( "select count(*) from mytest.test", classOf[Long]).toSingle()
    assert(count != None && count.get ==2 )

  }

  @Test(groups = Array("bigdecimal"))
  def testBigDecimal() {

    val qm = QueryManager(open = getConnection, logConnection = true)
    val dropDbSql = "drop database if exists mytest"
    qm.queryForUpdate(dropDbSql).executeUpdate
    val createDbSql = "create database if not exists mytest"
    qm.queryForUpdate(createDbSql).executeUpdate
    val createTableSql = "create table if not exists mytest.test(x Decimal(18,4))"
    qm.queryForUpdate(createTableSql).executeUpdate

    //note: BigDecimal must be java.math.BigDecimal not scala.bigDecimal
    try {
      qm.transaction() {transaction =>
        qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
          .parameterByName("x", new BigDecimal("1")).executeUpdate

        qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
          .parameterByName("x", new BigDecimal("1")).executeUpdate
        throw new Error(" let it fail during transaction")
      }
    }
    catch {
      case _:Error =>
    }

    //the inserted rows should have rollback.
    var count = qm.queryWithClass( "select count(*) from mytest.test", classOf[BigDecimal]).toSingle()
    assert(count != None  )
    assert( count.get.setScale(0, RoundingMode.HALF_EVEN).intValue == 0 )

    //do one more times
    qm.transaction() { transaction =>
      qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
        .parameterByName("x", 1).executeUpdate

      qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
        .parameterByName("x", 2).executeUpdate
    }
    count = qm.queryWithClass( "select count(*) from mytest.test", classOf[BigDecimal]).toSingle()
    assert(count != None && count.get.setScale(0, RoundingMode.HALF_EVEN).intValue() ==2 )

    val results = qm.queryWithClass("select *  from mytest.test", classOf[BigDecimal]).toList()
    assert(results.size == 2)

    assert( results.head.setScale(0, RoundingMode.HALF_EVEN).intValue == 1)
    assert(results.tail.head.setScale(1, RoundingMode.HALF_EVEN).doubleValue() == 2.0)

  }



  @Test(groups = Array("update", "query", "rowExtractor"))
  def testRowExtractor() {
    val qm = QueryManager(open = getConnection)
    val rowProcessor = new RowExtractor[(String, String)] {
      def extractRow(oneRow: Map[ColumnMetadata, Any]): (String, String) = {
        assert(oneRow.size ==2 )
        val schema = oneRow.filter(c => c._1.colLabel == "table_schema").head._2.toString
        val name   = oneRow.filter(c => c._1.colLabel == "table_name").head._2.toString
        (schema, name)
      }
    }

    val results = qm.query(" select table_schema, table_name" +
                           " from information_schema.tables limit 10", rowProcessor).toList()

    assert(results.size == 10)
    assert (results.filter(c => c._2.toUpperCase == "CHARACTER_SETS").size == 1)
    assert (results.filter(c => c._2.toUpperCase == "COLLATIONS").size == 1)
    assert (results.filter(c => c._1.toLowerCase == "information_schema").size == 10)
  }



  @Test(groups = Array("select", "query", "bigdecimal"))
  def testBigdecimal2() {
    val qm = QueryManager(open = getConnection)

    val bigDecimalValue= qm.queryWithClass(" select 1.0 from dual", classOf[BigDecimal] ).toSingle()
    val y = bigDecimalValue.get.setScale(1, java.math.RoundingMode.HALF_EVEN)
    assert(y.doubleValue() == 1.0)

  }



    @Test(groups = Array("select", "query", "datatype"))
  def testSelectDataType() {

    //long data type
    val qm = QueryManager(open = getConnection)
    val longValue = qm.queryWithClass(" select 1 from dual", classOf[Long] ).toSingle()
    assert(longValue.get == 1L)

    //double data type
    val dblValue= qm.queryWithClass(" select 1.0 from dual", classOf[Double] ).toSingle()
    assert(dblValue.get == 1.0)


    val bigDecimalValue= qm.queryWithClass(" select 1.0 from dual", classOf[BigDecimal] ).toSingle()
    assert(bigDecimalValue.get.setScale(1, java.math.RoundingMode.HALF_EVEN).doubleValue() == 1.0)



    //date type
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val dtValue= qm.queryWithClass(" select now() from dual", classOf[Date] ).toSingle()
    assert(formatter.format(dtValue.get) == formatter.format(new Date()) )


    val createDateTableSql = "create table if not exists mytest.testDate(dt DATETIME)"
    qm.queryForUpdate(createDateTableSql).executeUpdate
    val today = new Date()
    try {
      qm.queryForUpdate("INSERT INTO mytest.testDate(dt) values (:date) ")
        .parameterByName("date", today)
        .executeUpdate
    }
    catch {
      case x:Exception => x.printStackTrace(); assert(false, "failed to update date")
    }

     val dt = qm.queryWithClass("select dt from mytest.testDate where dt = :date ", classOf[Date])
               .parameterByName("date",  today).toSingle()

      assert(formatter.format(today) == formatter.format(dt.get) )

      // blob data type
    val dropDbSql = "drop database if exists mytest"
    qm.queryForUpdate(dropDbSql).executeUpdate
    val createDbSql = "create database if not exists mytest"
    qm.queryForUpdate(createDbSql).executeUpdate

    val createTableSql = "create table if not exists mytest.test2(x BLOB)"
    qm.queryForUpdate(createTableSql).executeUpdate

    val s = new QueryTest.SerializedObj("hi", new Date(), 100)
    qm.queryForUpdate("insert into mytest.test2(x) values (:x)")
      .parameterByName("x", s)
      .executeUpdate

    val rowProcessor = new RowExtractor[QueryTest.SerializedObj] {
      def extractRow(oneRow: Map[ColumnMetadata, Any]): QueryTest.SerializedObj = oneRow.head._2.asInstanceOf[QueryTest.SerializedObj]
    }

    val blobValue = qm.query(" select x from mytest.test2 limit 1", rowProcessor).toSingle()
    assert(blobValue.get.toString == s.toString)

    val blobValue2 = qm.queryWithClass(" select x from mytest.test2 limit 1", classOf[QueryTest.SerializedObj]).toSingle()
    assert(blobValue2.get.toString == s.toString)
  }

  @Test(groups = Array("Int-type"))
  def testLong2Int() {
    val qm = QueryManager(open = getConnection)
    //int type
    val intValue= qm.queryWithClass(" select 1 from dual", classOf[Int] ).toSingle()
    assert(intValue.get == 1)
  }


  @Test(groups = Array("null"))
  def testNullValue() {
    val qm = QueryManager(open = getConnection)
    //int type

    val createTableSql = "create table if not exists mytest.testNull(id Integer, x Date)"
    qm.queryForUpdate(createTableSql).executeUpdate
    qm.queryForUpdate("insert into mytest.testNull(id, x) values (1, :x)")
      .parameterByName("x", null)
      .executeUpdate

    val dateValue= qm.queryWithClass(" select x from mytest.testNull", classOf[Date] ).toSingle()
    assert(dateValue.get == null)
  }


  @Test(groups = Array("Precision and Scale"))
  def testPrecisionAndScales() {
    assert(false, "tests not implemented")
  }


  @Test(groups = Array("conn"))
  def testConnections() {
    val qm = QueryManager(open = getConnection, logConnection = true)
    for (i <- 0 until 100 ) {
      val value= qm.queryWithClass(" select 1 from dual", classOf[Int] ).toSingle()
    }
  }

  @Test(groups = Array("conn", "conn2"))
  def testConnection2() {

    val qm = QueryManager(open = getConnection, logConnection = true)
    val createTableSql = "create table if not exists mytest.test(x Integer)"
    qm.queryForUpdate(createTableSql).executeUpdate

    for (i <- 0 until 100 ) {
      val q = qm.queryForUpdate("update mytest.test set x = :x")
      q.parameterByName("x", 1)
      q.executeUpdate
   }

  }

  @Test(groups = Array("conn", "conn3"))
  def testConnection3() {

    val qm = QueryManager(open = getConnection, logConnection = true)
    val createTableSql = "create table if not exists mytest.test(x Integer)"
    qm.queryForUpdate(createTableSql).executeUpdate

    val q = qm.queryForUpdate("update mytest.test set x = :x")
    for (i <- 0 until 100 ) {
        q.parameterByName("x", 1)
        q.addBatch()
    }
    q.executeBatchUpdate
  }





  @Test(groups = Array("connection"))
  def testConnection() {
    try {
      val conn = getConnection
      conn.close()
    }
    catch {
      case ex:Exception => assert(false, "failed to get connection: " + ex.getMessage)
    }

  }


  def getConnection: Connection = {

    val userName = config.getString("db.username")
    val password = config.getString("db.password")
    val url = config.getString("db.driver.url")
/*

    println(" userName = " + userName)
    println(" password = " + password)
    println(" url = " + url)
*/

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = DriverManager.getConnection(url, userName, password)
    conn
  }


}
