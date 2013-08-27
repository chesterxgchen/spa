package com.xiaoguangchen.spa

import org.scalatest.{Tag, FunSpec}
import scala.collection.mutable.Stack
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
