package com.xiaoguangchen.spa

import scala.Predef._
import scala.Array

/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/17/13 8:25 PM
  */


trait Query[A] {

  def isolationLevel(isolation: Int): Query[A]

  def fetchSize(fetchSize: Int): Query[A]

  def toList(): List[A]

  def toSingle(): Option[A]

  def withIterator[T] (f: ( QueryIterator[A]) => T) :T

  def addBatch(): Query[A]

  def parameterByPos[T](pos: Int, param: T ): Query[A]

  def parameterByName[T](name: String, param: T): Query[A]

  def logSql(log: Boolean): Query[A]

  /**
   * execute SQL statement, if the statement generated the new id, return the newly generated key value
   * otherwise return the statement code
   *
   * here we assume that the key is Long
   *
   * @return 1) id value from resultset.getGeneratedKey if there are id generated;
   *         2) the row count for SQL Data Manipulation Language (DML) statements if no generated keys
   *         3) 0 for SQL statements that return nothing  if no generated keys
   * */
  def executeUpdate: Long

  def executeBatchUpdate: Array[Int]





}