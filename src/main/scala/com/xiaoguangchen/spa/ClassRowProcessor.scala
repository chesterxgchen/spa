package com.xiaoguangchen.spa

import com.xiaoguangchen.spa.annotation.Column
import java.lang.reflect._
import java.math.{BigDecimal, BigInteger}
import java.util.Date
import scala.Predef._
import scala.{None, AnyRef, Some}
import scala.Predef.Class
import scala.Some
import scala.Array
import collection.mutable
import java.sql.Blob
import java.io.{ByteArrayInputStream, ObjectInputStream, InputStream}


/**
 * Chester Chen (chesterxgchen@yahoo.com
 * Date: 12/23/12
 * Time: 4:41 PM
 *
 */

/**
 * Extract the class from One database row.
 * This class use reflection to populate the object instance of the class.
 *
 * The class is instantiate and populated with the following ways:
 *
 * 1) use default constructor, then via setter method
 * 2) use constructor directly, if the supplied row columns are less than the constructors arguments,
 *    then the rest of the arguments must have proper values
 *
 * The setter methods are determined as the following:
 *
 *    a) the setter method with only one argument and the column matches the column label or Name.
 *    b) the setter method with only one argument and name matches the set<ColumnLabel>
 *    c) the setter method with only one argument and name matches the <ColumnLabel>_$eq
 *

 * The resultClass must be a
 *
 * a) regular java/scala class or
 * b) static inner java class or
 * c) a scala class in companion object
 *
 * @param resultClass --
 * @tparam T
 */

class ClassRowProcessor[T] (resultClass: Class[T]) extends RowExtractor[T] {

  var m_resultClass = resultClass


  def extractRow(oneRow: Map[ColumnMetadata, Any]): T = {



    def getBytes(buf: Array[Byte]) = {
      println(" extract a byte array")
      val value =  if (buf != null) {
        val objectIn = new ObjectInputStream(new ByteArrayInputStream(buf))
        objectIn.readObject
      } else null

      if (value == null) null else value.asInstanceOf[T]
    }

    println(" start to extract")
    try {
      require(m_resultClass != null, "result class is null")

      val columnCount = oneRow.size
      if (columnCount == 1) {
        val value = oneRow.head._2
        if (value == null) return null.asInstanceOf[T]


        println(" m_resultClass = " +  value.getClass.getSimpleName)
        println(" value.getClass.getSimpleName = " +  value.getClass.getSimpleName)

      val returnValue =
          (m_resultClass.getSimpleName, value.getClass.getSimpleName) match {
            case ("Long", "BigDecimal") => value.asInstanceOf[BigDecimal].longValue()
            case ("Double", "BigDecimal") => value.asInstanceOf[BigDecimal].doubleValue()
            case ("int", "Long") => value.asInstanceOf[Long].toInt
            case ("Integer", "Long") => value.asInstanceOf[Long].toInt
            case ("Double", "Float") => value.asInstanceOf[Double].toFloat
            case ("BigDecimal", "Long") => new BigDecimal(value.asInstanceOf[Long])
            case ("BigDecimal", "Int") => new BigDecimal(value.asInstanceOf[Int])
            case ("BigDecimal", "Double") => new BigDecimal(value.asInstanceOf[Double])
            case (_, "byte[]") => getBytes(value.asInstanceOf[Array[Byte]])
            case (_, "Blob") => {val blob = value.asInstanceOf[Blob]; getBytes(blob.getBytes(1,blob.length.toInt)) }

            case _ => value
          }

        println(" return value = " + returnValue)
          return returnValue.asInstanceOf[T]
      }
     println(" not simple class")
      val methods = m_resultClass.getDeclaredMethods
      val fields = m_resultClass.getDeclaredFields
      val annFields = fields.filter( p => (p.getAnnotation(classOf[Column]) != null) )

      val constructors = m_resultClass.getDeclaredConstructors.filter(p =>p.getModifiers == Modifier.PUBLIC)
      val consWithAnn = constructors.filter (c => (!c.getParameterAnnotations.filter( p => !p.filter( a => a.annotationType() eq classOf[Column]).isEmpty ).isEmpty))

      if (consWithAnn.isEmpty) { // no parameters with Column Annotation

        val defaultCtrs = constructors.filter(p => p.getParameterTypes.size == 0)
        if (!defaultCtrs.isEmpty) {
          val t = m_resultClass.newInstance
          setValues(oneRow, methods, annFields, t)
          return t
        }
        else
          throw new QueryException("failed to extract row for class: " + m_resultClass.getName + " no default constructor ")
      }
      else {
        println(" constructing constructor")
        val ctor = consWithAnn(0).asInstanceOf[Constructor[T]]
        val args = getConstructorArgs(ctor, oneRow)
        val t= ctor.newInstance(args: _*)
        val setters = getSetterMethods(oneRow, methods, annFields)
        for ((md, m) <- setters) setValue(t, m, oneRow.get(md).get)

        return t

      }

      throw new QueryException("failed to extract row for class: " + m_resultClass.getName)
    }
    catch {
      case e: InstantiationException => {
        throw new QueryException("Failed to instantiate class " + m_resultClass.getName + " make sure the class has default constructor.", e)
      }
      case e: IllegalAccessException => {
        throw new QueryException("Failed to access class " + m_resultClass.getName, e)
      }
    }

  }


  //////////////////////////////////////////////////////////////////////////////////////////


  private def getConstructorArgs(ctor: Constructor[T], oneRow: Map[ColumnMetadata, Any]): Seq[AnyRef] = {
    val parAnns = ctor.getParameterAnnotations
    val size = parAnns.size
    val args = mutable.ListBuffer.empty[AnyRef]


    for (i <- 0 until size; annArray = parAnns(i)) {
      val colAnns = annArray.filter( a => (a.annotationType() eq classOf[Column]))
      if (colAnns.isEmpty) {
        args += null
      }
      else {
        val ann = colAnns(0).asInstanceOf[Column]
        for ((md, value ) <- oneRow; if (ann.value().equals(md.colName) || ann.value.equals(md.colLabel)) )
          args += (if (value == null || value == None) null else value.asInstanceOf[AnyRef])
      }
    }


    args

  }

  private def setValues(oneRow: Map[ColumnMetadata, Any], methods: Array[Method], annFields: Array[Field], t: T) {
    for ((md, value) <- oneRow ) {
      val setter = getSetterMethod(methods, annFields, md)
      setter match {
        case Some(m) => setValue(t, m, value)
        case None => throw new QueryException("There is no setter or Column annotation for Column " + md.colName + " or alias " + md.colLabel + " in class " + m_resultClass.getName)
      }
    }
  }

  private def getSetterMethods(oneRow:    Map[ColumnMetadata, Any],
                               methods:   Array[Method],
                               annFields: Array[Field]): mutable.Map[ColumnMetadata, Method] = {
    val results = mutable.Map[ColumnMetadata, Method]()
    for ((md, value) <- oneRow ) {
         val setter = getSetterMethod(methods, annFields, md)
         if (setter != None) results += (md -> setter.get)
    }

    results
  }

  private def getSetterMethod(methods: Array[Method], annFields: Array[Field], md: ColumnMetadata ): Option[Method] = {

    val am = methods.filter(m =>
      ( m.getAnnotation(classOf[Column]) != null &&
        (m.getAnnotation(classOf[Column]).value().equalsIgnoreCase(md.colName) ||
          m.getAnnotation(classOf[Column]).value().equalsIgnoreCase(md.colLabel)) &&
        m.getParameterTypes.length == 1)
    )

    if (!am.isEmpty) return Some(am(0))

    // no setter method with the Column annotation
    // check if there is a setter method for the corresponding annotated field
    val af = annFields.filter( f => ( f.getAnnotation(classOf[Column]).value().equalsIgnoreCase(md.colName) ||
                               f.getAnnotation(classOf[Column]).value().equalsIgnoreCase(md.colLabel) ))

    if (!af.isEmpty) {
      val setters = methods.filter(m => ( m.getName.equalsIgnoreCase("set" + af(0).getName) ||
        m.getName.equalsIgnoreCase(af(0).getName + "_$eq") ) &&
        m.getParameterTypes.length == 1)

      if (!setters.isEmpty) Some(setters(0)) else None
    }  else {
      val setters = methods.filter(m => m.getName.equalsIgnoreCase("set" + md.colLabel))
      if (!setters.isEmpty) Some(setters(0)) else None
    }

  }

  private def setValue(t :T, setter: Method, value: Any) {
    try {
      setter.invoke(t, value.asInstanceOf[AnyRef])
    }
    catch {
      case e: IllegalAccessException => {
        throw new QueryException("Failed to access method " + setter.getName + " value = " + value, e)
      }
      case e: IllegalArgumentException => {
        throw new QueryException("Failed to invoke method " + setter.getName + " value = " + value, e)
      }
      case e: InvocationTargetException => {
        throw new QueryException("Failed to invoke method " + setter.getName + " value = " + value, e)
      }
    }
  }

}

