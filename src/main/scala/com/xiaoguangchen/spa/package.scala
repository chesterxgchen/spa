package com.xiaoguangchen

import java.sql.Timestamp
import java.util.{Calendar, Date}

/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 8/22/13

 */
package object spa {


  case class ParsedSql(sql: String, parameterPositions: Map[Int, SQLParameter]) {

    def + (other: ParsedSql) : ParsedSql = {
      val part1Size  = this.parameterPositions.size

      new ParsedSql( this.sql + other.sql,
        this.parameterPositions ++ (other.parameterPositions map (a => (a._1 + part1Size) -> a._2 )))
    }

  }

  implicit class SqlContext(sc : StringContext) {
    private val positionedParameters =Map[Int, SQLParameter]()

    def parameters (): Map[Int, SQLParameter] = positionedParameters

    def sql( args:Any*): ParsedSql = {

      val filterParts: Seq[String] = sc.parts.filter(!_.trim.isEmpty)
      if (filterParts.isEmpty && args.size == 1) {
        ParsedSql(args(0).toString, parameters())
      }
      else {
        val parameters = (args.indices zip args).foldLeft(Map[Int, SQLParameter]())( (acc, a) =>  acc + (a._1 -> toSqlParameter(a._2)))
        val placeHolders = args.map( a=> "?")
        val sql = sc.s(placeHolders:_*)
        val raw = sc.raw(placeHolders:_*)
        ParsedSql(sql, parameters)
      }
    }
  }

  private[spa]  def toSqlParameter[T] (param: T ): SQLParameter = {

    val sqlParam:SQLParameter =
      if (param == null)
        new SQLParameter(param.asInstanceOf[AnyRef],java.sql.Types.NULL)
      else {
        param match {
          case t:Timestamp => new SQLParameter(t,java.sql.Types.TIMESTAMP)
          case d:Date =>      new SQLParameter(new Timestamp(d.getTime),java.sql.Types.TIMESTAMP)
          case c:Calendar =>  new SQLParameter(new Timestamp(c.getTime.getTime),java.sql.Types.TIMESTAMP)
          case d1:scala.BigDecimal =>  new SQLParameter(new java.math.BigDecimal(d1.toString()),java.sql.Types.JAVA_OBJECT)

          case s: Any =>    new SQLParameter(s.asInstanceOf[AnyRef],java.sql.Types.JAVA_OBJECT)
          case _ => throw new IllegalArgumentException(" unable to handle parameter " + param )
        }
      }

    sqlParam
  }


}
