package com.xiaoguangchen.spa


class ExecuteQueryException(message: String, cause: Throwable) extends QueryException(message: String, cause: Throwable) {
   def this(message: String) {
    this (message, null)
  }

  def this(cause: Throwable) {
    this (null, cause)
  }
}

