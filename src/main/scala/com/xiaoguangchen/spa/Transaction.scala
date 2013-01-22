package com.xiaoguangchen.spa

/**
 * Chester Chen (chesterxgchen@yahoo.com)
 * User: Chester Chen
 * Date: 12/25/12
 * Time: 5:25 AM
 *
 */

import java.sql._


class Transaction(val connection: Connection)  {
  val autocommit = connection.getAutoCommit
  def begin() {
    connection.setAutoCommit(false)
  }

  def commit() {
    connection.commit()
    connection.setAutoCommit(autocommit)
  }

  def rollback() {
    connection.rollback()
    try {
      connection.setAutoCommit(autocommit)
    } catch {
      case _: SQLException =>
    }
  }

  def transaction[T](f: Transaction => T) = f(this)

}