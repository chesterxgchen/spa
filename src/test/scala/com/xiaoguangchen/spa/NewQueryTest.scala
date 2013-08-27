package com.xiaoguangchen.spa

import org.scalatest.{Tag, FunSpec}
import scala.collection.mutable.Stack
import scala.Predef._
import java.sql.Connection
import com.xiaoguangchen.spa.annotation.Column
import scala.Tuple3
import scala.Some
import java.text.SimpleDateFormat
import java.util.Date


/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 8/5/13
 */

//constructor annotation
case class Coffee(@Column("COF_NAME") name:  String,
                  @Column("SUP_ID")   supId: Int,
                  @Column("PRICE")    price: Double)


case class CoffeePrice( name:  String,
                        price: Double)



class NewQueryTest extends BaseTest with FunSpec {

  describe("A Stack") {

    it("should pop values in last-in-first-out order") {
      val stack = new Stack[Int]
      stack.push(1)
      stack.push(2)
      assert(stack.pop() === 2)
      assert(stack.pop() === 1)
    }

    it("should throw NoSuchElementException if an empty stack is popped") {
      val emptyStack = new Stack[Int]
      intercept[NoSuchElementException] {
        emptyStack.pop()
      }
    }
  }

  describe(" SqlContext test") {

    it(" sql context will convert String to SqlContext ") {
      val table = "dummy"
      val parsedSql = sql"select count(*) from information_schema.tables where table_name=$table "
      assert(parsedSql.sql.trim === "select count(*) from information_schema.tables where table_name=?")
      assert(parsedSql.parameterPositions.get(0).map(a => a.parameter) === Some("dummy"))
    }

    it(" sql context with combined variables ") {
      val firstName = "firstName"
      val lastName = "lastName"
      val parsedSql = sql"select $firstName, $lastName from usertable t where t.username = ${firstName + lastName}"
      assert(parsedSql.sql.trim === "select ?, ? from usertable t where t.username = ?")
      assert(parsedSql.parameterPositions.get(0).map(a => a.parameter) === Some(firstName ))
      assert(parsedSql.parameterPositions.get(1).map(a => a.parameter) === Some(lastName))
      assert(parsedSql.parameterPositions.get(2).map(a => a.parameter) === Some(firstName + lastName))
    }

    it(" sql context can be split into multiple lines ") {
        val name = "name"
        val email = "email"
        val parsedSql = sql"select $name, " + sql"$email from usertable t where t.username = $name"

        assert(parsedSql.sql.trim === "select ?, ? from usertable t where t.username = ?")
        assert(parsedSql.parameterPositions.get(0).map(a => a.parameter) === Some(name ))
        assert(parsedSql.parameterPositions.get(1).map(a => a.parameter) === Some(email))
        assert(parsedSql.parameterPositions.get(2).map(a => a.parameter) === Some(name))
    }
  }

  describe("MySQL Test") {
    val qm = QueryManager(open = getMySQLConnection, logConnection = true )

    it(" select SQL should return number of tables in mySQL information.schema ") {
      val table = "dummy2"
      qm.transaction() { trans =>
        implicit val transaction = Some(trans)
        val parsedSql = sql"select count(*) from information_schema.tables where table_name=$table"
        val x = qm.selectQuery(parsedSql).toSingle[Int]
        assert (x === Some(0))
      }
    }

    it(" select SQL without explicit transaction, and internal transaction should be in place ") {
      val table = "dummy3"
      val parsedSql = sql"select count(*) from information_schema.tables where table_name=$table"
      val x = qm.selectQuery(parsedSql).toSingle[Int]
      assert (x === Some(0))
    }

    it(" select SQL return List of String ") {
      val parsedSql = sql"select table_name from information_schema.tables"
      val x = qm.selectQuery(parsedSql).toList[String]
      assert (x.size > 0)
    }
  }

   describe("Update test") {
     val qm = QueryManager(open = getMySQLConnection)

     it ("test update ") {
       qm.transaction() { implicit trans  =>
         val dropDbSql = sql"drop database if exists mytest"
         qm.updateQuery(dropDbSql).executeUpdate

         val createDbSql = sql"create database if not exists mytest"
         qm.updateQuery(createDbSql).executeUpdate
       }

       val db="mytest"
       val table="test"
       val selectTableSql = sql" select count(*) from information_schema.tables where " +
                            sql" table_schema = $db and table_name = $table"

       val count = qm.selectQuery(selectTableSql).toSingle[Long]
       assert(count.get == 0)

       val createTableSql = sql"create table if not exists mytest.test(x Integer)"
       qm.updateQuery(createTableSql).executeUpdate

       val count1 = qm.selectQuery(selectTableSql).toSingle[Long]
       assert(count1.isDefined)
       assert(count1.get >= 1) //in case of multi-thread tests
    }
   }

   describe("transaction Test") {
     val qm = QueryManager(open = getMySQLConnection)


     it("test transaction rollback",Tag("debug") ) {


       val db="mytest"
       val dbCountSql = sql"select count(TABLE_SCHEMA) from information_schema.tables where table_schema = $db"

       //first drop database;
       val dropDbSql = sql"drop database if exists mytest"
       qm.updateQuery(dropDbSql).logSql(true).executeUpdate

       val dbCount = qm.selectQuery(dbCountSql).toSingle[Long]
       assert (dbCount === Some(0))

       intercept[PreparedStatementException] { //expecting

         qm.transaction() { implicit trans =>

           val createDbSql = sql"create database if not exists mytest"
           qm.updateQuery(createDbSql).executeUpdate

           //drop database synatax error, the whole transaction should rollack, therefore the database should don't be created.
           val dropDbSql2 = sql"drop database"
           qm.updateQuery(dropDbSql2).executeUpdate //this should fail
         }
       }

       val dbCount2 = qm.selectQuery(dbCountSql).toSingle[Long]
       assert (dbCount2 === Some(0))
     }


     it (" update transaction roll-back") {
       val dropTableSql = sql"drop table if exists mytest.test"
       qm.updateQuery(dropTableSql).logSql(true).executeUpdate

       val createTableSql = sql"create table mytest.test(x Integer)"
       qm.updateQuery(createTableSql).logSql(true).executeUpdate

       qm.updateQuery(sql"insert into mytest.test(x) values (1) ").executeUpdate

       val value = qm.selectQuery(sql"select x from mytest.test").logSql(true).toSingle[Long]
       assert(value == Some(1))

       intercept[ExecuteQueryException] {
         qm.transaction() { implicit trans =>

           qm.updateQuery(sql"update mytest.test set x = 2").executeUpdate

           throw new ExecuteQueryException("see if I can rollback")
         }
       }

       val value2 = qm.selectQuery(sql"select x from mytest.test").toSingle[Long]
       assert(value2 === Some(1))
       qm.transaction() { implicit trans =>
        qm.updateQuery(sql"update mytest.test set x = 2").executeUpdate
       }

       val value3 = qm.selectQuery(sql"select x from mytest.test").toSingle[Long]
       assert(value3 === Some(2))

     }


   }

   describe("batch test ") {

     it(" batch update then followed by select query") {


       val qm = QueryManager(open = getMySQLConnection)

       val dropDbSql = sql"drop database if exists mytest"
       qm.updateQuery(dropDbSql).executeUpdate

       val createDbSql = sql"create database if not exists mytest"
       qm.updateQuery(createDbSql).executeUpdate



       val createTableSql = sql"create table if not exists mytest.test(x Integer, y Integer)"
       qm.updateQuery(createTableSql).executeUpdate

       val prefixSql = sql"insert into mytest.test (x, y) values(?, ?) "
       val q = qm.batchUpdateQuery(prefixSql)

       //index is zero-based
       val size = 10
       for (i <- 0 until size ) {
         val pa = Map(0 -> i, 1 -> i*20) // x  is 0, y  is 1
         q.addBatch(pa)
       }
       q.executeBatchUpdate

       val z = qm.selectQuery(sql"select x,y from mytest.test").toList[(Int, Int)]
       assert (z.size === size)
       assert (z(0) ===(0,0))
       assert (z(1) ===(1,20))
       assert (z(size-1) ===(size-1,20*(size-1)))
     }

     it("batch test using different data types return tuple3") {

       val qm = QueryManager(open = getMySQLConnection)

       val dropDbSql = sql"drop database if exists mytest"
       qm.updateQuery(dropDbSql).executeUpdate

       val createDbSql = sql"create database if not exists mytest"
       qm.updateQuery(createDbSql).executeUpdate

       val createTableSql = sql"create table if not exists mytest.test(x DECIMAL(16), y DECIMAL(32), z varchar(30))"
       qm.updateQuery(createTableSql).executeUpdate

       val prefixSql = sql"insert into mytest.test (x, y, z) values(?, ?, ?) "
       val q = qm.batchUpdateQuery(prefixSql)

       //index is zero-based
       val size = 10
       for (i <- 0 until size ) {
         val pa = Map(0 -> i, 1 -> i*20, 2 ->s"value of $i") // x  is 0, y  is 1, z is 2
         q.addBatch(pa)
       }
       q.executeBatchUpdate

       val z = qm.selectQuery(sql"select x,y, z from mytest.test").toList[(Long, Long, String)]
       assert (z.size === size)

       assert (Tuple3(0, 0, "0") === (0,0, "0"))
       assert (z(0)._1 === 0L)
       assert (z(0)._2 === 0L)
       assert (z(0)._3 ==="value of 0")

       assert (z(1).isInstanceOf[(Long, Long, String)])

       assert (z(1) === (1,20,"value of 1"))
       assert (z(2) === (2,40,"value of 2"))
       assert (z(size-1) ===(size-1,20*(size-1),s"value of ${size-1}"))
     }
   }

  describe("test select query ") {

    it("constructor with Annotation") {
      val qm = QueryManager(open = getMySQLConnection)

      val coffees = prepareCoffee(qm)

      val results = qm.selectQuery(sql" select * from mytest.COFFEES ").toList[Coffee]
      assert ( results.size === coffees.size)

    }

    it("test WithIterator ") {

      val qm = QueryManager(open = getMySQLConnection)
      val coffees = prepareCoffee(qm)

      // use Column Annotation on the parameters of the constructor
      val q = qm.selectQuery(sql" select * from mytest.COFFEES ")

      val results =  q.withIterator { it: Iterator[Option[Coffee]] =>
        it.foldLeft(List[Coffee]())( (acc, a) => a.get :: acc).reverse
      }
      assert ( results.size === coffees.size)
    }

    it("test row extractor ") {

      val qm = QueryManager(open = getMySQLConnection)
      val coffees = prepareCoffee(qm)
      val rowProcessor = new RowExtractor[CoffeePrice] {

        def extractRow(oneRowCols: Map[ColumnMetadata, Any]): CoffeePrice = {

          import scala.math.BigDecimal.javaBigDecimal2bigDecimal
          //there should only two columns
          assert(oneRowCols.size ==2 )
          val colValues = oneRowCols.map(a => a._1.colLabel -> a._2)
          val name = colValues.get("COF_NAME").getOrElse("NA").toString
          val price = colValues.get("PRICE").getOrElse(0).asInstanceOf[java.math.BigDecimal].toDouble

          CoffeePrice(name, price)
        }
      }
      // use Column Annotation on the parameters of the constructor
      val results = qm.selectQuery(sql" select COF_NAME, PRICE from mytest.COFFEES ", Some(rowProcessor)).toList[CoffeePrice]
      assert ( results.size === coffees.size)

    }

    it("test select query simple data types with mySQL syntax") {
      {
        val qm = QueryManager(open = getMySQLConnection)
        val longValue = qm.selectQuery(sql" select 1 from dual").toSingle[Long]
        assert(longValue.get === 1L)

        val dblValue= qm.selectQuery(sql"select 1.0 from dual").toSingle[Double]
        assert(dblValue.get ===1.0)

        val flValue= qm.selectQuery(sql"select 1.0 from dual").toSingle[Float]
        assert(flValue.get ===1.0)


        val intValue= qm.selectQuery(sql"select 1 from dual").toSingle[Int]
        assert(intValue.get === 1)

        val bigDecimalValue= qm.selectQuery(sql"select 1.0 from dual").toSingle[BigDecimal]
        assert(bigDecimalValue.get=== BigDecimal(1.0))

        val stValue= qm.selectQuery(sql"select 'string' from dual").toSingle[String]
        assert(stValue.get=== "string")

        //date type note: mysql specific syntax
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val dtValue= qm.selectQuery(sql"select now() from dual"  ).toSingle[Date]
        assert(formatter.format(dtValue.get) == formatter.format(new Date()) )


        val createDateTableSql = sql"create table if not exists mytest.testDate(dt DATETIME)"
        qm.updateQuery(createDateTableSql).executeUpdate

        val today = new Date()
        qm.updateQuery(sql"INSERT INTO mytest.testDate(dt) values ($today) ").executeUpdate

        val dt = qm.selectQuery(sql"select dt from mytest.testDate where dt = $today ").toSingle[Date]
        assert(formatter.format(today) == formatter.format(dt.get) )
      }

    }

    it("test select query with tuple data types") {
      {
        val qm = QueryManager(open = getMySQLConnection)
        val tuple2Value = qm.selectQuery(sql" select 1, '2' from dual").toSingle[(Int, String)]
        assert(tuple2Value.get === (1, "2"))

        val date = new Date()
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val tuple3Value= qm.selectQuery(sql"select 1.0, '2', now() from dual").toSingle[(Double, String, Date)]
        assert(tuple3Value != None)

        val (x,y,z) = tuple3Value.get
        assert((x,y) === (1.0, "2"))
        assert( formatter.format(z) === formatter.format(date))

        val tuple4Value = qm.selectQuery(sql" select 1,2,3,4  from dual").toSingle[(Int,Int, Int,Int   )]
        assert(tuple4Value.get === ( 1,2,3,4 ))

        val tuple5Value = qm.selectQuery(sql" select 1,2,3,4,5   from dual").toSingle[(Int, Int, Int,Int, Int )]
        assert(tuple5Value.get === ( 1,2,3,4,5  ))

        val tuple6Value = qm.selectQuery(sql" select 1,2,3,4,5,6  from dual").toSingle[(Int, Int, Int,Int, Int, Int  )]
        assert(tuple6Value.get === ( 1,2,3,4,5,6))

        val tuple7Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7  from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int )]
        assert(tuple7Value.get === ( 1,2,3,4,5,6,7))

        val tuple8Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8  from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int )]
        assert(tuple8Value.get === ( 1,2,3,4,5,6,7,8 ))

        val tuple9Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8,9 from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int, Int )]
        assert(tuple9Value.get === ( 1,2,3,4,5,6,7,8,9))
      }

    }


  }

  describe("test select query with annotations") {

      val qm = QueryManager(open = getMySQLConnection)
      val coffees = prepareCoffee(qm)

      it("constructor with Annotation") {
        val results = qm.selectQuery(sql" select * from mytest.COFFEES ").toList[Coffee]
        assert ( results.size === coffees.size)
      }
      // other types of annotation is no longer supported

  }

  def prepareCoffee(qm: QueryManager) : List[Coffee] = {

    val dropDbSql = sql"drop database if exists mytest"
    qm.updateQuery(dropDbSql).executeUpdate
    val createDbSql = sql"create database if not exists mytest"
    qm.updateQuery(createDbSql).executeUpdate
    val createTableSql = sql"create table if not exists mytest.COFFEES(COF_NAME varchar(30), SUP_ID INTEGER, PRICE DECIMAL(10,4))"
    qm.updateQuery(createTableSql).executeUpdate

    val coffees = List(Coffee("Colombian", 101, 7.99),
      Coffee("Colombian Decaf", 101, 8.99),
      Coffee("French Roast Decaf", 49, 9.99))

    //use a transaction so that all updates will be in one transaction and the connection will be closed at the end of transaction
    qm.transaction() {
      implicit trans =>
        for (c <- coffees) {
          qm.updateQuery(sql"insert into mytest.COFFEES (COF_NAME, SUP_ID, PRICE) values (${c.name}, ${c.supId}, ${c.price} )")
            .executeUpdate
        }
    }

    coffees
  }

  def getMySQLConnection: Option[Connection] = {
        val userName = config.getString("db.username")
        val password = config.getString("db.password")
        val url = config.getString("db.driver.url")
        val driver = config.getString("db.driver.name")
        QueryManager.getConnection(driver, url, userName, password)
    }
}
