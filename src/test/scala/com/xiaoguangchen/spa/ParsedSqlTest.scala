package com.xiaoguangchen.spa

import org.scalatest.FunSpec

/**
 *
 * Created by chester on 11/3/15.
 */
class ParsedSqlTest extends FunSpec {

  describe("test ParsedSql") {
    it("should be able to parse select query") {
      val parsedSql : ParsedSql = sql"SELECT spcname FROM pg_tablespace limit 1"
      assert(parsedSql.sql === "SELECT spcname FROM pg_tablespace limit 1")
      assert(parsedSql.parameterPositions.isEmpty)
    }
  }

}
