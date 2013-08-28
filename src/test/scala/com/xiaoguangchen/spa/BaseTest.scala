package com.xiaoguangchen.spa

import com.typesafe.config._
import com.xiaoguangchen.spa.annotation.Column

/**
  * author: Chester Chen (chester@tingatech.com) 
  * Date: 1/23/13 9:15 AM
  */
class BaseTest {
  val config = ConfigFactory.load()
}


//constructor annotation
case class Coffee(@Column("COF_NAME") name:  String,
                  @Column("SUP_ID")   supId: Int,
                  @Column("PRICE")    price: Double)


case class CoffeePrice( name:  String,
                        price: Double)

