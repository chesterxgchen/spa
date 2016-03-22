package com.xiaoguangchen.spa

import org.scalatest.FunSpec
import scala.Predef._
import scala.Tuple3
import scala.Some
import java.text.SimpleDateFormat
import java.util.Date


class PostgresTest extends BaseTest with FunSpec {


  if (database == PostgreSQL) {

    describe("Update test") {
      val qm = QueryManager(open = getConnection)

      it("test update ") {
        val table = "spa_test"

        val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
          sql" and tablename = $table"

        val count = qm.selectQuery(selectTableSql).toSingle[Long]
        if (count.get > 0) {
          qm.updateQuery(sql" drop table spa_test ").executeUpdate
        }
        val count2 = qm.selectQuery(selectTableSql).toSingle[Long]
        assert(count2.get === 0)

        val createTableSql = sql"create table if not exists spa_test(x Integer)"
        qm.updateQuery(createTableSql).executeUpdate

        val count1 = qm.selectQuery(selectTableSql).toSingle[Long]
        assert(count1.isDefined)
        assert(count1.get >= 1) //in case of multi-thread tests
      }


      describe(" test return auto generated key is not supported ") {

        // note: Postgres will append RETURNING to the origin SQL if the  "Statement.RETURN_GENERATED_KEYS" is used regardless the select, create delete or update
        // Postgres SQL doesn't has good support for returning generated keys
        // If we specified RETURN_GENERATED_KEYS, the Posgres will return all the inserted columns and values in the generatedKeys resultset
        // and there is no distinction on which column is generated or non-generated, making it hard to dynamically determine
        // the generated value. The only way is to tell SPA the auto-generated column name, which is a bit difficult to do in general
        // so I decide to not support return generated Key for postgres

        val table = "spa_test2"

        qm.transaction() {
          implicit trans =>

            val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
              sql" and tablename = $table"

            val count = qm.selectQuery(selectTableSql).toSingle[Long]
            if (count.get > 0) {
              qm.updateQuery(sql" drop table $table" ).executeUpdate
            }
            val count2 = qm.selectQuery(selectTableSql).toSingle[Long]
            assert(count2.get === 0)

            //qm.updateQuery(sql" drop table spa_test ").executeUpdate
            val createTableSql = sql"create table if not exists spa_test2 ( id  SERIAL,  x Integer)"
            qm.updateQuery(createTableSql).executeUpdate

        }

         val id1 = qm.updateQuery(sql" INSERT INTO spa_test2(x) values (3) ").executeUpdate
        println("id1 = " + id1)
        assert(id1 === 1)

        val id2 = qm.updateQuery(sql" INSERT INTO spa_test2(x) values (4) ").executeUpdate
        println("id2 = " + id2)
        assert(id2 === 2)

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

        val createTableSql = sql"create table if not exists spa_test(x Integer, y Integer)"
        qm.updateQuery(createTableSql).executeUpdate

        val prefixSql = sql"insert into test (x, y) values(?, ?) "
        val q = qm.batchUpdateQuery(prefixSql)

        //index is zero-based
        val size = 10
        for (i <- 0 until size) {
          val pa = Map(0 -> i, 1 -> i * 20) // x  is 0, y  is 1
          q.addBatch(pa)
        }
        q.executeBatchUpdate

        val z = qm.selectQuery(sql"select x,y from test").toList[(Int, Int)]
        assert(z.size === size)
        assert(z(0) ===(0, 0))
        assert(z(1) ===(1, 20))
        assert(z(size - 1) ===(size - 1, 20 * (size - 1)))
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

        val createTableSql = sql"create table if not exists spa_test(x DECIMAL(16), y DECIMAL(32), z varchar(30))"
        qm.updateQuery(createTableSql).executeUpdate

        val prefixSql = sql"insert into test (x, y, z) values(?, ?, ?) "
        val q = qm.batchUpdateQuery(prefixSql)

        //index is zero-based
        val size = 10
        for (i <- 0 until size) {
          val pa = Map(0 -> i, 1 -> i * 20, 2 -> s"value of $i") // x  is 0, y  is 1, z is 2
          q.addBatch(pa)
        }
        q.executeBatchUpdate

        val z = qm.selectQuery(sql"select x,y, z from test").toList[(Long, Long, String)]
        assert(z.size === size)

        assert(Tuple3(0, 0, "0") ===(0, 0, "0"))
        assert(z(0)._1 === 0L)
        assert(z(0)._2 === 0L)
        assert(z(0)._3 === "value of 0")

        assert(z(1).isInstanceOf[(Long, Long, String)])

        assert(z(1) ===(1, 20, "value of 1"))
        assert(z(2) ===(2, 40, "value of 2"))
        assert(z(size - 1) ===(size - 1, 20 * (size - 1), s"value of ${size - 1}"))
      }
    }



    describe("test select query ") {

      it("constructor with Annotation") {

        val qm = QueryManager(open = getConnection, true)

        val coffees = prepareCoffee(qm)

        val results = qm.selectQuery(sql" select * from COFFEES ").toList[Coffee]
        assert(results.size === coffees.size)

      }


      it("test WithIterator ") {

        val qm = QueryManager(open = getConnection)

        val coffees = prepareCoffee(qm)

        // use Column Annotation on the parameters of the constructor
        val q = qm.selectQuery(sql" select * from COFFEES ")

        val results = q.withIterator {
          it: Iterator[Option[Coffee]] =>
            it.foldLeft(List[Coffee]())((acc, a) => a.get :: acc).reverse
        }
        assert(results.size === coffees.size)
      }


      it("test row extractor ") {

        val qm = QueryManager(open = getConnection)
        val coffees = prepareCoffee(qm)
        val rowProcessor = new RowExtractor[CoffeePrice] {

          def extractRow(oneRowCols: Map[ColumnMetadata, Any]): CoffeePrice = {
            //there should only two columns
            assert(oneRowCols.size == 2)
            val colValues = oneRowCols.map(a => a._1.colLabel -> a._2)
            val name = colValues.get("COF_NAME").getOrElse("NA").toString
            val price = colValues.get("PRICE").getOrElse(0).asInstanceOf[Int].toDouble

            CoffeePrice(name, price)
          }
        }
        // use Column Annotation on the parameters of the constructor
        val results = qm.selectQuery(sql" select COF_NAME, PRICE from COFFEES ", Some(rowProcessor)).toList[CoffeePrice]
        assert(results.size === coffees.size)

      }

      it("test select query simple data types with mySQL syntax") {
        {
          val qm = QueryManager(open = getConnection)
          val longValue = qm.selectQuery(sql" select 1  ").toSingle[Long]
          assert(longValue.get === 1L)

          val dblValue = qm.selectQuery(sql"select 1.0  ").toSingle[Double]
          assert(dblValue.get === 1.0)

          val flValue = qm.selectQuery(sql"select 1.0  ").toSingle[Float]
          assert(flValue.get === 1.0)


          val intValue = qm.selectQuery(sql"select 1  ").toSingle[Int]
          assert(intValue.get === 1)

          val bigDecimalValue = qm.selectQuery(sql"select 1.0  ").toSingle[BigDecimal]
          assert(bigDecimalValue.get === BigDecimal(1.0))

          val stValue = qm.selectQuery(sql"select 'string' ").toSingle[String]
          assert(stValue.get === "string")

          val formatter = new SimpleDateFormat("yyyy-MM-dd")
          val dtValue = qm.selectQuery(sql"select now() ").toSingle[Date]
          assert(formatter.format(dtValue.get) == formatter.format(new Date()))


          val table = "testdate"
          val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
            sql" and tablename = $table"

          val count = qm.selectQuery(selectTableSql).toSingle[Long]
          if (count.get > 0) {
            qm.updateQuery(sql" drop table testdate ").executeUpdate
          }

          val createDateTableSql = sql"create table if not exists testdate(dt DATE)"
          qm.updateQuery(createDateTableSql).executeUpdate

          val today = new Date()
          qm.updateQuery(sql"INSERT INTO testdate(dt) values ($today) ").executeUpdate

          val dt = qm.selectQuery(sql"select dt from testdate where dt = $today ").toSingle[Date]
          assert(formatter.format(today) == formatter.format(dt.get))
        }

      }


      it("test select query with tuple data types") {
        {
          val qm = QueryManager(open = getConnection)
          val tuple2Value = qm.selectQuery(sql" select 1, '2' ").toSingle[(Int, String)]
          assert(tuple2Value.get ===(1, "2"))

          val date = new Date()
          val formatter = new SimpleDateFormat("yyyy-MM-dd")
          val tuple3Value = qm.selectQuery(sql"select 1.0 as A, '2' as B, now() as C  ").toSingle[(Double, String, Date)]
          assert(tuple3Value != None)

          val (x, y, z) = tuple3Value.get
          assert((x, y) ===(1.0, "2"))
          assert(formatter.format(z) === formatter.format(date))

          val tuple4Value = qm.selectQuery(sql" select 1,2,3,4  ").toSingle[(Int, Int, Int, Int)]
          assert(tuple4Value.get ===(1, 2, 3, 4))

          val tuple5Value = qm.selectQuery(sql" select 1,2,3,4,5  ").toSingle[(Int, Int, Int, Int, Int)]
          assert(tuple5Value.get ===(1, 2, 3, 4, 5))

          val tuple6Value = qm.selectQuery(sql" select 1,2,3,4,5,6  ").toSingle[(Int, Int, Int, Int, Int, Int)]
          assert(tuple6Value.get ===(1, 2, 3, 4, 5, 6))

          val tuple7Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7  ").toSingle[(Int, Int, Int, Int, Int, Int, Int)]
          assert(tuple7Value.get ===(1, 2, 3, 4, 5, 6, 7))

          val tuple8Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8  ").toSingle[(Int, Int, Int, Int, Int, Int, Int, Int)]
          assert(tuple8Value.get ===(1, 2, 3, 4, 5, 6, 7, 8))

          val tuple9Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8,9  ").toSingle[(Int, Int, Int, Int, Int, Int, Int, Int, Int)]
          assert(tuple9Value.get ===(1, 2, 3, 4, 5, 6, 7, 8, 9))
        }

      }


    }


    describe("transaction Test") {

      it("test transaction ") {

        val qm = QueryManager(open = getConnection)
        qm.transaction() {
          implicit trans =>
            val table = "testdate"
            val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
              sql" and tablename = $table"

            val count = qm.selectQuery(selectTableSql).toSingle[Long]
            if (count.get > 0) {
              qm.updateQuery(sql" drop table testdate ").executeUpdate
            }

            val count2 = qm.selectQuery(selectTableSql).toSingle[Long]
            assert(count2.get == 0)


            val createTableSql = sql"create table if not exists testdate(x INTEGER)"
            qm.updateQuery(createTableSql).executeUpdate

            val count3 = qm.selectQuery(selectTableSql).toSingle[Long]
            assert(count3.get > 0)

        }
      }

      it(" update transaction roll-back") {

        val qm = QueryManager(open = getConnection)

        val table = "test"
        val selectTableSql = sql" select count(*) from pg_tables where schemaname='public' " +
          sql" and tablename = $table"

        val count = qm.selectQuery(selectTableSql).toSingle[Long]
        if (count.get > 0) {
          qm.updateQuery(sql" drop table test  ").executeUpdate
        }

        val createTableSql = sql"create table if not exists spa_test(x INTEGER)"
        qm.updateQuery(createTableSql).executeUpdate

        qm.updateQuery(sql"INSERT INTO spa_test(x) values (1) ").executeUpdate

        val xvalue = qm.selectQuery(sql"select x from test").toSingle[Int]
        assert(xvalue == Some(1))

        intercept[ExecuteQueryException] {
          qm.transaction() {
            implicit trans =>
            //update first
              qm.updateQuery(sql"update spa_test set x = 2").executeUpdate
              //then throw exception after update
              throw new ExecuteQueryException("see if I can rollback")
          }
        }

        println("now trying to select again")
        val xvalue2 = qm.selectQuery(sql"select x from spa_test").toSingle[Int]

        println(" xvalue2= " + xvalue2)
        assert(xvalue2 === Some(1))

        qm.transaction() {
          implicit trans =>
            qm.updateQuery(sql"update spa_test set x = 2").executeUpdate
        }

        val xvalue3 = qm.selectQuery(sql"select x from spa_test").toSingle[Int]
        assert(xvalue3 === Some(2))

      }

    }

/*


    describe("Random Tests") {
      it ("temp test") {
        val qm = QueryManager(open = getConnection)
        val r = qm.selectQuery(sql"select * FROM data_mining.election92 where county = 'Carroll' LIMIT 10", Some(new RecordRowExtractor())).toList
        println("r = " + r)

      }
    }

*/

    def prepareCoffee(qm: QueryManager): List[Coffee] = {

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
      val createTableSql = sql"create table if not exists if not exists coffees(COF_NAME varchar(30), SUP_ID INTEGER, PRICE DECIMAL(10,4))"
      qm.updateQuery(createTableSql).executeUpdate

      println(" create table if not exists  done ")

      val coffees = List(Coffee("Colombian", 101, 7.99), Coffee("Colombian Decaf", 101, 8.99), Coffee("French Roast Decaf", 49, 9.99))

      println(" insert coffees ")
      qm.transaction() {
        implicit trans =>
          for (c <- coffees) {
            qm.updateQuery(sql"insert into COFFEES (COF_NAME, SUP_ID, PRICE) values (${c.name}, ${c.supId}, ${c.price} )")
              .executeUpdate
          }
      }
      println(" return coffees ")
      coffees
    }

  }
}

class RecordRowExtractor( ) extends RowExtractor[Seq[(String, String)]] {
  def extractRow(oneRowCols: Map[ColumnMetadata, Any]): Seq[(String, String)] = {
    val colValues : Seq[(ColumnMetadata, Any)] = oneRowCols.toList.sortBy(a => a._1.colPos)
    colValues.map(a => a._1.colLabel -> (if (a._2 == null) "NA" else a._2.toString))
  }
}

