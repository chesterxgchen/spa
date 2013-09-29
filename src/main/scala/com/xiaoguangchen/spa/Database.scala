package com.xiaoguangchen.spa

/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 8/26/13

 */
trait Database

object MySQL         extends Database
object PostgreSQL      extends Database
object Oracle        extends Database
object OtherDatabase extends Database

