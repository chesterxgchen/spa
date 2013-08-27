package com.xiaoguangchen.spa

import com.xiaoguangchen.spa.annotation.Column
import java.lang.reflect._
import java.math.BigDecimal
import scala.Predef._
import scala.{None, AnyRef}
import scala.Predef.Class
import scala.reflect.{ClassTag}
import scala.reflect._
import scala.reflect.runtime.universe._

import scala.Some
import scala.Array
import collection.mutable
import java.sql.Blob
import java.io.{ByteArrayInputStream, ObjectInputStream}


/**
 * Chester Chen (chesterxgchen@yahoo.com
 * Date: 12/23/12
 * Time: 4:41 PM
 *
 */

/**
 * Extract the class from One database row.
 * This class use reflection to populate the object instance of the class unless it is Tuple2-9 or simple single column class.
 *
 * @tparam T
 */

class ClassRowProcessor[T: ClassTag : TypeTag ] extends RowExtractor[T] {

   val typeArgs = typeOf[T] match { case TypeRef(_, _, args) => args }
   val m_resultClass =  implicitly[ClassTag[T]].runtimeClass

  private def convertValue(value: Any, targetClassName: String): Any = {

    (targetClassName, value.getClass.getSimpleName) match {
      case ("Long", "BigDecimal") => value.asInstanceOf[BigDecimal].longValue()
      case ( d1, "BigDecimal") if d1 == "Double" || d1 == "double" => value.asInstanceOf[BigDecimal].doubleValue()
      case ( i,  "Long")       if i  == "Int"    || i  == "int"  || i == "Integer"   => value.asInstanceOf[Long].toInt
      case ("Double", "Float") => value.asInstanceOf[Double].toFloat
      case ("BigDecimal", "Long") => scala.BigDecimal(new BigDecimal(value.asInstanceOf[Long]))
      case ("BigDecimal", "Int") => scala.BigDecimal(new BigDecimal(value.asInstanceOf[Int]))
      case ("BigDecimal", "Double") => scala.BigDecimal(new BigDecimal(value.asInstanceOf[Double]))
      case ("BigDecimal", "BigDecimal") if m_resultClass.getName != value.getClass.getName => {
        scala.BigDecimal(value.asInstanceOf[java.math.BigDecimal])
      }
      case (_, "byte[]") => getBytes(value.asInstanceOf[Array[Byte]])
      case (_, "Blob") => {
        val blob = value.asInstanceOf[Blob]
        getBytes(blob.getBytes(1, blob.length.toInt))
      }

      case _ => value
    }
  }


  private def getBytes(buf: Array[Byte]) = {
    val value =  if (buf != null) {
      val objectIn = new ObjectInputStream(new ByteArrayInputStream(buf))
      objectIn.readObject
    } else null

    if (value == null) null else value.asInstanceOf[T]
  }

  def extractRow(oneRow: Map[ColumnMetadata, Any]): T = {


    def extractClassRow: T = {
      val constructors = m_resultClass.getDeclaredConstructors.filter(p => p.getModifiers == Modifier.PUBLIC)

      def ctorAnnFilter (c: Constructor[_]) : Boolean = {
        !c.getParameterAnnotations.filter(p => !p.filter(a => a.annotationType() eq classOf[Column]).isEmpty).isEmpty
      }

      val consWithAnn = constructors.filter(c => ctorAnnFilter(c))
      if (!consWithAnn.isEmpty) {
        val ctor = consWithAnn(0).asInstanceOf[Constructor[T]]
        val args = getConstructorArgs(ctor, oneRow)
        ctor.newInstance(args: _*)
      }
      else
        throw new QueryException("failed to extract row for class: " + m_resultClass.getName)
   }


    try {
      require(m_resultClass != null, "result class is null")
      val columnCount = oneRow.size

      columnCount match {

        case 1 =>
          val value = oneRow.head._2
          if (value == null) return null.asInstanceOf[T]

          val returnValue = convertValue(value,m_resultClass.getSimpleName)
          returnValue.asInstanceOf[T]

        case n if n == typeArgs.size =>      //tupleN
         val values = oneRow.map( m=> m._1.colPos ->  convertValue(m._2, typeArgs(m._1.colPos).toString)   )
         val result = m_resultClass.getSimpleName match {
           case "Tuple2"  => Tuple2(values.get(0).get, values.get(1).get)
           case "Tuple3"  => Tuple3(values.get(0).get, values.get(1).get,values.get(2).get)
           case "Tuple4"  => Tuple4(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get)
           case "Tuple5"  => Tuple5(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get, values.get(4).get)
           case "Tuple6"  => Tuple6(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get,values.get(4).get, values.get(5).get)
           case "Tuple7"  => Tuple7(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get,values.get(4).get, values.get(5).get,values.get(6).get)
           case "Tuple8"  => Tuple8(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get,values.get(4).get, values.get(5).get,values.get(6).get,values.get(7).get)
           case "Tuple9"  => Tuple9(values.get(0).get, values.get(1).get,values.get(2).get,values.get(3).get,values.get(4).get, values.get(5).get,values.get(6).get,values.get(7).get,values.get(8).get)
           case _  =>  extractClassRow
         }

         result.asInstanceOf[T]

        case _ => extractClassRow
      }

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
    val parTypes =  ctor.getParameterTypes
    val parInfos = parAnns zip parTypes

    val size = parAnns.size
    val args = mutable.ListBuffer.empty[AnyRef]

    for (i <- 0 until size; infoArray = parInfos(i); annArray = infoArray._1; parType = infoArray._2) {
      val colAnns = annArray.filter( a => (a.annotationType() eq classOf[Column]))
      if (colAnns.isEmpty) {
        args += null
      }
      else {
        val annValue = colAnns(0).asInstanceOf[Column].value().toUpperCase
        val tName = parType.getSimpleName

        for ((md, value ) <- oneRow; if annValue.toUpperCase.equals(md.colName.toUpperCase) || annValue.equals(md.colLabel.toUpperCase) ) {
          args += (if (value == null || value == None) null else convertValue(value, tName).asInstanceOf[AnyRef])
        }

      }
    }

    args
  }


}

