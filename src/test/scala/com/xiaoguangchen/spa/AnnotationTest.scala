package com.xiaoguangchen.spa

import annotation.Column
import java.lang.reflect.{Constructor, Modifier, Field, Method}
import java.util.Date
import org.testng.annotations.Test
import reflect.BeanProperty


/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/18/13 8:57 PM
  */
class AnnotationTest extends BaseTest{


  class FieldAnn () {
    @Column("name")  var name:String = null
    @Column("checksum")  var checksum:Long = -1
  }

  class MethodAnn () {
    var name:String = null

    @Column("name")
    def setName(s: String)  = this.name = s
  }

  class ParamAnn (@Column("schema") val schema:String,
                  @Column("name") val name:String,
                  @Column("create_time") val createTime: Date = null)

  class ParamCtor {

    @BeanProperty
    var schema:String = null

    @BeanProperty
    var name : String = null
  }


  @Test (groups = Array("annotation"))
  def testFieldAnnotation() {
    val md = new FieldAnn()

    val fields = md.getClass.getDeclaredFields
    val annFields = for (f <-  fields; fieldAnn = f.getAnnotation(classOf[Column]); if (fieldAnn != null) ) yield  f
    assert (annFields.size == 2)

    for (f <- annFields) {
      val fieldAnn = f.getAnnotation(classOf[Column])
      assert(f.getName == fieldAnn.value())
    }
  }


  @Test (groups = Array("annotation"))
  def testMethodAnnotation() {
    val md = new MethodAnn()
    val methods = md.getClass.getMethods
    val annMethods = for (m <- methods; colAnn = m.getAnnotation(classOf[Column]); if (colAnn != null) )  yield m
    assert(!annMethods.isEmpty)
    assert(annMethods(0).getName  == "setName")
    assert(annMethods(0).getAnnotation(classOf[Column]).value == "name")

  }



  @Test (groups = Array("annotation"))
  def testValParamAnnotation() {

    val md = new ParamAnn("schema1", "name1", new Date())

    val constructors = md.getClass.getDeclaredConstructors.filter(p =>p.getModifiers == Modifier.PUBLIC)
    val consWithAnn = constructors.filter (c => (!c.getParameterAnnotations.filter( p => !p.filter( a => a.annotationType() eq classOf[Column]).isEmpty ).isEmpty))
    assert (!consWithAnn.isEmpty)
    val defaultCtrs = constructors.filter(p => p.getParameterTypes.length ==0)
    assert (defaultCtrs.isEmpty)

    val ctor = consWithAnn(0).asInstanceOf[Constructor[ParamAnn]]

    val parAnns = ctor.getParameterAnnotations
    val params = ctor.getParameterTypes
    assert (params.size == parAnns.size)

    for (i <- 1 until params.size) {
       val  pClass = params(i)
       assert(!parAnns(i).isEmpty)

       val pAnn = parAnns(i)(0).asInstanceOf[Column]
       if(pAnn.value() == "name") {
         assert(pClass == md.name.getClass)
       }
        else if(pAnn.value() == "schema") {
          assert(pClass == md.schema.getClass)
        }
       else if(pAnn.value() == "create_time") {
         assert(pClass == md.createTime.getClass)
       }

    }
  }


  @Test (groups = Array("constructor"))
  def testDefaultCtr() {
    val md = new ParamCtor
    val defaultCtrs = getDefaultConstructors(md)
    assert (!defaultCtrs.isEmpty)
  }

  @Test (groups = Array("constructor", "constructor2"))
  def testDefaultCtr2() {
    val md = new ParamCtor
    val defaultCtrs = getDefaultConstructors(md)
    assert (!defaultCtrs.isEmpty)

    try {
     defaultCtrs(0).newInstance(this)
     assert(true, "instance created with default constructor ")
    }
    catch {
      case t:Throwable => assert(false, "can't create instance with default constructor: " + t.getMessage)
    }
  }



  private def getDefaultConstructors(md: AnnotationTest.this.type#ParamCtor):Array[Constructor[_]] = {
    val constructors = md.getClass.getDeclaredConstructors.filter(p => p.getModifiers == Modifier.PUBLIC)
    val consWithAnn = constructors.filter(c => (!c.getParameterAnnotations.filter(p => !p.filter(a => a.annotationType() eq classOf[Column]).isEmpty).isEmpty))
    assert(consWithAnn.isEmpty)

    def defaultFilter(c: Constructor[_], clazz: Class[_]): Boolean = {
      val types: Array[Class[_]] = c.getParameterTypes

      if (types.size == 0)
        true
      else {
        val container = clazz.getDeclaringClass
        container != null && types.size == 1 && types(0) == container
      }
    }

    constructors.filter(p => defaultFilter(p, md.getClass))
  }





}
