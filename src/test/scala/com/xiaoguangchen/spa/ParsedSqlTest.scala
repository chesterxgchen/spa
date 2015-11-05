package com.xiaoguangchen.spa

import org.scalatest.FunSpec

/**
 *
 * Created by chester on 11/3/15.
 */
class ParsedSqlTest extends FunSpec {

  describe("test ParsedSql") {
    it("should be able to parse select query") {
      println("\n\ntest 1\n ")
      val parsedSql : ParsedSql = sql"SELECT spcname FROM pg_tablespace limit 1"
      assert(parsedSql.sql === "SELECT spcname FROM pg_tablespace limit 1")
      assert(parsedSql.parameterPositions.isEmpty)
    }

    it("should be able to parse select query2") {
      println("\n\ntest 2\n ")
      val limit = 1
      val parsedSql : ParsedSql = sql"SELECT spcname FROM pg_tablespace limit $limit"
      assert(parsedSql.sql === "SELECT spcname FROM pg_tablespace limit ?")
      assert(!parsedSql.parameterPositions.isEmpty)
      assert(parsedSql.parameterPositions.get(0) === Some(new SQLParameter(1, java.sql.Types.JAVA_OBJECT)))
    }

    it("should be able to parse select query3") {
      println("\n\ntest 3\n ")
      val sqlStr = "SELECT spcname FROM pg_tablespace limit 1"
      val parsedSql : ParsedSql = sql"$sqlStr"
      assert(parsedSql.sql === "SELECT spcname FROM pg_tablespace limit 1")
      assert(parsedSql.parameterPositions.isEmpty)
    }

    it("should be able to parse select query4") {
      println("\n\ntest 4\n ")
      val limit = 1
      val sqlStr = s"SELECT spcname FROM pg_tablespace limit $limit"
      val parsedSql : ParsedSql = sql"$sqlStr"
      assert(parsedSql.sql === "SELECT spcname FROM pg_tablespace limit 1")
      assert(parsedSql.parameterPositions.isEmpty)
    }


  }

}
