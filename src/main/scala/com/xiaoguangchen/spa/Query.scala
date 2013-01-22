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

  def executeUpdate: Int

  def executeBatchUpdate: Array[Int]





}