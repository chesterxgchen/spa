package com.xiaoguangchen.spa

class QueryException(message: String, cause: Throwable) extends RuntimeException(message: String, cause: Throwable) {

  def this(message: String) {
    this(message, null);
  }

  def this(cause: Throwable) {
    this (null, cause)
  }
}

