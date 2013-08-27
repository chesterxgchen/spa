package com.xiaoguangchen.spa

import org.scalatest.{Tag, FunSpec}
import scala.Predef._
import java.sql.Connection
import scala.Tuple3
import scala.Some
import java.text.SimpleDateFormat
import java.util.Date


class PostgresTest extends BaseTest with FunSpec {

   describe("Update test") {
     val qm = QueryManager(open = getConnection)

     it ("test update ") {
       val table="test"

       val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
                            sql" and tablename = $table"

       val count = qm.selectQuery(selectTableSql).toSingle[Long]
       if (count.get > 0) {
         qm.updateQuery(sql" drop table test ").executeUpdate
       }
       val count2 = qm.selectQuery(selectTableSql).toSingle[Long]
       assert(count2.get === 0)

       val createTableSql = sql"create table test(x Integer)"
       qm.updateQuery(createTableSql).executeUpdate

       val count1 = qm.selectQuery(selectTableSql).toSingle[Long]
       assert(count1.isDefined)
       assert(count1.get >= 1) //in case of multi-thread tests
     }
   }


  describe("batch test ") {

    it(" batch update then followed by select query") {

      val qm = QueryManager(open = getConnection)


      val table = "test"
      val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
        sql" and tablename = $table"

      val count = qm.selectQuery(selectTableSql).toSingle[Long]
      if (count.get > 0) {
        qm.updateQuery(sql" drop table test ").executeUpdate
      }

      val createTableSql = sql"create table test(x Integer, y Integer)"
      qm.updateQuery(createTableSql).executeUpdate

      val prefixSql = sql"insert into test (x, y) values(?, ?) "
      val q = qm.batchUpdateQuery(prefixSql)

      //index is zero-based
      val size = 10
      for (i <- 0 until size ) {
        val pa = Map(0 -> i, 1 -> i*20) // x  is 0, y  is 1
        q.addBatch(pa)
      }
      q.executeBatchUpdate

      val z = qm.selectQuery(sql"select x,y from test").toList[(Int, Int)]
      assert (z.size === size)
      assert (z(0) ===(0,0))
      assert (z(1) ===(1,20))
      assert (z(size-1) ===(size-1,20*(size-1)))
    }

    it("batch test using different data types return tuple3") {

      val qm = QueryManager(open = getConnection)

      val table = "test"
      val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
        sql" and tablename = $table"

      val count = qm.selectQuery(selectTableSql).toSingle[Long]
      if (count.get > 0) {
        qm.updateQuery(sql" drop table test ").executeUpdate
      }

      val createTableSql = sql"create table test(x DECIMAL(16), y DECIMAL(32), z varchar(30))"
      qm.updateQuery(createTableSql).executeUpdate

      val prefixSql = sql"insert into test (x, y, z) values(?, ?, ?) "
      val q = qm.batchUpdateQuery(prefixSql)

      //index is zero-based
      val size = 10
      for (i <- 0 until size ) {
        val pa = Map(0 -> i, 1 -> i*20, 2 ->s"value of $i") // x  is 0, y  is 1, z is 2
        q.addBatch(pa)
      }
      q.executeBatchUpdate

      val z = qm.selectQuery(sql"select x,y, z from test").toList[(Long, Long, String)]
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

      val qm = QueryManager(open = getConnection, true)

      val coffees = prepareCoffee(qm)

      val results = qm.selectQuery(sql" select * from COFFEES ").toList[Coffee]
      assert ( results.size === coffees.size)

    }


    it("test WithIterator ") {

      val qm = QueryManager(open = getConnection)

      val coffees = prepareCoffee(qm)

      // use Column Annotation on the parameters of the constructor
      val q = qm.selectQuery(sql" select * from COFFEES ")

      val results =  q.withIterator { it: Iterator[Option[Coffee]] =>
        it.foldLeft(List[Coffee]())( (acc, a) => a.get :: acc).reverse
      }
      assert ( results.size === coffees.size)
    }


    it("test row extractor ") {

      val qm = QueryManager(open = getConnection)
      val coffees = prepareCoffee(qm)
      val rowProcessor = new RowExtractor[CoffeePrice] {

        def extractRow(oneRowCols: Map[ColumnMetadata, Any]): CoffeePrice = {
          //there should only two columns
          assert(oneRowCols.size ==2 )
          val colValues = oneRowCols.map(a => a._1.colLabel -> a._2)
          val name = colValues.get("COF_NAME").getOrElse("NA").toString
          val price = colValues.get("PRICE").getOrElse(0).asInstanceOf[Int].toDouble

          CoffeePrice(name, price)
        }
      }
      // use Column Annotation on the parameters of the constructor
      val results = qm.selectQuery(sql" select COF_NAME, PRICE from COFFEES ", Some(rowProcessor)).toList[CoffeePrice]
      assert ( results.size === coffees.size)

    }

    it("test select query simple data types with mySQL syntax") {
      {
        val qm = QueryManager(open = getConnection)
        val longValue = qm.selectQuery(sql" select 1  ").toSingle[Long]
        assert(longValue.get === 1L)

        val dblValue= qm.selectQuery(sql"select 1.0  ").toSingle[Double]
        assert(dblValue.get ===1.0)

        val flValue= qm.selectQuery(sql"select 1.0  ").toSingle[Float]
        assert(flValue.get ===1.0)


        val intValue= qm.selectQuery(sql"select 1  ").toSingle[Int]
        assert(intValue.get === 1)

        val bigDecimalValue= qm.selectQuery(sql"select 1.0  ").toSingle[BigDecimal]
        assert(bigDecimalValue.get=== BigDecimal(1.0))

        val stValue= qm.selectQuery(sql"select 'string' ").toSingle[String]
        assert(stValue.get=== "string")

        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val dtValue= qm.selectQuery(sql"select now() "  ).toSingle[Date]
        assert(formatter.format(dtValue.get) == formatter.format(new Date()) )


        val table = "testdate"
        val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
          sql" and tablename = $table"

        val count = qm.selectQuery(selectTableSql).toSingle[Long]
        if (count.get > 0) {
          qm.updateQuery(sql" drop table testdate ").executeUpdate
        }

        val createDateTableSql = sql"create table testdate(dt DATE)"
        qm.updateQuery(createDateTableSql).executeUpdate

        val today = new Date()
        qm.updateQuery(sql"INSERT INTO testdate(dt) values ($today) ").executeUpdate

        val dt = qm.selectQuery(sql"select dt from testdate where dt = $today ").toSingle[Date]
        assert(formatter.format(today) == formatter.format(dt.get) )
      }

    }


    it("test select query with tuple data types") {
      {
        val qm = QueryManager(open = getConnection)
        val tuple2Value = qm.selectQuery(sql" select 1, '2' ").toSingle[(Int, String)]
        assert(tuple2Value.get === (1, "2"))

        val date = new Date()
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val tuple3Value= qm.selectQuery(sql"select 1.0 as A, '2' as B, now() as C  ").toSingle[(Double, String, Date)]
        assert(tuple3Value != None)

        val (x,y,z) = tuple3Value.get
        assert((x,y) === (1.0, "2"))
        assert( formatter.format(z) === formatter.format(date))

        val tuple4Value = qm.selectQuery(sql" select 1,2,3,4  ").toSingle[(Int,Int, Int,Int   )]
        assert(tuple4Value.get === ( 1,2,3,4 ))

        val tuple5Value = qm.selectQuery(sql" select 1,2,3,4,5  ").toSingle[(Int, Int, Int,Int, Int )]
        assert(tuple5Value.get === ( 1,2,3,4,5  ))

        val tuple6Value = qm.selectQuery(sql" select 1,2,3,4,5,6  ").toSingle[(Int, Int, Int,Int, Int, Int  )]
        assert(tuple6Value.get === ( 1,2,3,4,5,6))

        val tuple7Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7  ").toSingle[(Int, Int, Int,Int, Int, Int, Int )]
        assert(tuple7Value.get === ( 1,2,3,4,5,6,7))

        val tuple8Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8  ").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int )]
        assert(tuple8Value.get === ( 1,2,3,4,5,6,7,8 ))

        val tuple9Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8,9  ").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int, Int )]
        assert(tuple9Value.get === ( 1,2,3,4,5,6,7,8,9))
      }

    }


  }

  describe("transaction Test") {

    it("test transaction ") {

      val qm = QueryManager(open = getConnection)
      qm.transaction() { implicit trans =>
        val table = "testdate"
        val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
          sql" and tablename = $table"

        val count = qm.selectQuery(selectTableSql).toSingle[Long]
        if (count.get > 0) {
          qm.updateQuery(sql" drop table testdate ").executeUpdate
        }

        val count2 = qm.selectQuery(selectTableSql).toSingle[Long]
        assert (count2.get == 0)


        val createTableSql = sql"create table testdate(x INTEGER)"
        qm.updateQuery(createTableSql).executeUpdate

        val count3 = qm.selectQuery(selectTableSql).toSingle[Long]
        assert (count3.get > 0)

      }
    }

    it (" update transaction roll-back") {

      val qm = QueryManager(open = getConnection)

      val table = "test"
      val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
        sql" and tablename = $table"

      val count = qm.selectQuery(selectTableSql).toSingle[Long]
      if (count.get > 0) {
        qm.updateQuery(sql" drop table test  ").executeUpdate
      }

      val createTableSql = sql"create table test(x INTEGER)"
      qm.updateQuery(createTableSql).executeUpdate

      qm.updateQuery(sql"INSERT INTO test(x) values (1) ").executeUpdate

      val xvalue = qm.selectQuery(sql"select x from test").toSingle[Int]
      assert(xvalue == Some(1))

      intercept[ExecuteQueryException] {
        qm.transaction() { implicit trans =>
          //update first
          qm.updateQuery(sql"update test set x = 2").executeUpdate
          //then throw exception after update
          throw new ExecuteQueryException("see if I can rollback")
        }
      }

      println("now trying to select again")
      val xvalue2 = qm.selectQuery(sql"select x from test").toSingle[Int]

      println(" xvalue2= " + xvalue2)
      assert(xvalue2 === Some(1))

      qm.transaction() { implicit trans =>
       qm.updateQuery(sql"update test set x = 2").executeUpdate
      }

      val xvalue3 = qm.selectQuery(sql"select x from test").toSingle[Int]
      assert(xvalue3 === Some(2))

    }

  }


  def prepareCoffee(qm: QueryManager) : List[Coffee] = {

   println(" select table ")
    val table = "coffees"
    val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
      sql" and tablename = $table"

    val count = qm.selectQuery(selectTableSql).toSingle[Long]
    if (count.get > 0) {
      println(" drop table ")
      qm.updateQuery(sql" drop table COFFEES ").executeUpdate
      println(" drop table done")
    }

    println(" create table ")
    val createTableSql = sql"create table coffees(COF_NAME varchar(30), SUP_ID INTEGER, PRICE DECIMAL(10,4))"
    qm.updateQuery(createTableSql).executeUpdate

    println(" create table done ")

    val coffees = List(Coffee("Colombian", 101, 7.99), Coffee("Colombian Decaf", 101, 8.99), Coffee("French Roast Decaf", 49, 9.99))

    println(" insert coffees ")
    qm.transaction() { implicit trans =>
        for (c <- coffees) {
          qm.updateQuery(sql"insert into COFFEES (COF_NAME, SUP_ID, PRICE) values (${c.name}, ${c.supId}, ${c.price} )")
            .executeUpdate
        }
    }
    println(" return coffees ")
    coffees
  }

  def getConnection: Option[Connection] = {
    val userName = config.getString("db.postgres.username")
    val password = config.getString("db.postgres.password")
    val url = config.getString("db.postgres.driver.url")
    val driver = config.getString("db.postgres.driver.name")

    QueryManager.getConnection(driver, url, userName, password)
  }
}
