package com.xiaoguangchen.spa

import java.io.Serializable

class SQLParameter(val parameter: Any, val sqlType: Int = java.sql.Types.NULL) extends Serializable {
  override def toString: String = {
    parameter  + ":"  + sqlType
  }

  override def equals(obj: scala.Any): Boolean = {
     obj match {
       case other: SQLParameter =>
         this.parameter == other.parameter &&
           this.sqlType == other.sqlType
       case _ =>
         false
     }

  }
}
