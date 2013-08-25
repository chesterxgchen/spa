package com.xiaoguangchen.spa

import scala.reflect.ClassTag
import java.lang.reflect.Method
import scala.Predef._
import scala.Some

/**
 * User: Chester Chen (chesterxgchen@yahoo.com)
 * Date: 7/14/13

 */

//column name -> setter method name/field Name mapping
case class ColumnMapping[A](mappings:(String, String)*)(implicit tag: ClassTag[A]) {


  for (m <- mappings) validate(m)
  val columnMethodMaps = getSetterMethod()


///////////////////////////////////////////////////////////////////////////////
  private def validate(mapping: (String, String)) {
    val (columnName, name ) = mapping
    require(columnName != null && !columnName.trim.isEmpty )
    require(name != null && !name.trim.isEmpty )
  }

  private def getSetterMethod(): Map[String, Method]  = {

    val runTimeClass = tag.runtimeClass
    val methods = runTimeClass.getDeclaredMethods.filter(_.getParameterTypes.length == 1)

    def addMethod(acc: Map[String, Method], term: (String, String) ) :  Map[String, Method] = {
      val (columnName, name ) = term
      val method = methods.find(m => m.getName.equalsIgnoreCase(name) ||
        m.getName.equalsIgnoreCase("set" + name) ||
        m.getName.equalsIgnoreCase(name + "_$eq"))
      if (method.isDefined) {
        acc.get(columnName.toLowerCase) match {
          case None    =>  acc + (columnName -> method)
          case Some(m) =>  //ignore
        }
      }

      acc
    }

    val colMethodMap = Map[String, Method]()
    mappings.foldLeft(colMethodMap)(addMethod)

    colMethodMap
  }



}