package com.xiaoguangchen.spa

import com.typesafe.config._
import com.xiaoguangchen.spa.annotation.Column
import java.sql.Connection

/**
  * author: Chester Chen
  * Date: 1/23/13 9:15 AM
  */
class BaseTest {
  val config = ConfigFactory.load()
  val database = getDatabaseVendor


  def getDatabaseVendor:Database = {
    config.getString("db.vendor")  match {
      case "mysql" => MySQL
      case "postgres" => PostgreSQL
      case "oracle" =>   Oracle
      case _ => OtherDatabase
    }

  }

  def getConnectionProps  : (String, String, String, String ) = {

    val (usernameKey, passwordKey, urlKey, driverKey ) = getDatabaseVendor match  {
      case MySQL =>
        ( "db.mysql.username" , "db.mysql.password",  "db.mysql.driver.url" , "db.mysql.driver.name")
      case PostgreSQL =>
        ( "db.postgres.username" , "db.postgres.password",  "db.postgres.driver.url" , "db.postgres.driver.name")
      case Oracle =>
        ( "db.oracle.username" , "db.oracle.password",  "db.oracle.driver.url" , "db.oracle.driver.name")
      case OtherDatabase =>
        ( "db.username" , "db.password",  "db.driver.url" , "db.driver.name")
    }

    val userName = config.getString(usernameKey)
    val password = config.getString(passwordKey)
    val url = config.getString(urlKey)
    val driver = config.getString(driverKey)

    (userName, password, url, driver)
  }


  def getConnection: Option[Connection] = {
    val (userName ,  password ,  url,  driver) = getConnectionProps
    QueryManager.getConnection(driver, url, userName, password)
  }
}


//constructor annotation
case class Coffee(@Column("COF_NAME") name:  String,
                  @Column("SUP_ID")   supId: Int,
                  @Column("PRICE")    price: Double)


case class CoffeePrice( name:  String,
                        price: Double)

