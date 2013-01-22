package com.xiaoguangchen.spa

/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/18/13 6:26 PM
  */
class PreparedStatementException(message: String, cause: Throwable) extends QueryException(message: String, cause: Throwable) {
  def this(message: String) {
    this (message, null)
  }
  def this(cause: Throwable) {
    this (null, cause)
  }

}
