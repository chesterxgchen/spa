SPA 
===

SPA or spa -- Scala Persistence API, a scala JDBC wrapper

Chester Chen


SPA is a JDBC wrapper to make query database easier. It is not Object-Relation mapping library. It's not a Function-Relation
mapping library either. There is no special query language, joins etc, just plain native SQL. SQL is enough.

The following are some of the features support by SPA:



* The spa is resource safe: queries will with close the connection/statement/resultSet without user to worry about the leaking of resources. 
* flexible ways of handling results via toList, toSingle and withIterator methods
* Use can also customize results processing via Customized RowExtractor
* support batch update
* automatic sql logging when query failed.
* support transaction  -- updates withing the transaction will be commit or rollback together.

not supported

* Store Procedure call, the store procedure might be added when needs arise.

not intend to support

* connection Pool



## Background

This work was originally inspired by the JPA (java persistence api) native query, which has the ability to select database results by simply pass the class and native SQL.
The original JPA doesn't support batch update, iterator, transaction and other features in SPA. The transaction related feature is inspired by Querulous. The transaction construct is borrowed from there.
The latest changes (0.2, 0.1 was never officially released) removed the mutable setters APIs from Query, the new API is more functional oriented than the 0.1 version. 

### Use at your own risk

Use at your own risk. I have used this library for my own projects. 


## License

The project is under Apache License


## Requirements

The 0.2 version requires scala 2.10. The Scala-2.10 features such as string interpolation and implicit class are used. 






## Usage Examples

In the following, I am going to use the same example other libraries with Coffee. 
I also uses the existing mySQL database schema tables. 





### Select Query features

#### Select Single Value

```   
   def getConnection : Connection = ...

   val qm = QueryManager(open = getConnection )
   
   val table = "dummy"
   
   val parsedSql = sql"select count(*) from information_schema.tables where table_name=$table"

   val count = qm.selectQuery(parsedSql).toSingle[Int]
   
```   
 In above example, getConnection is function that returns JDBC connection. I created a QueryManager instance with the call-by-name argument
open.
 
 The sql"..."  is Scala 2.10 string interpolation which parsed the SQL syntax and associated parameter "table", 
 
 the query returns a single Option[Int] value and assign value to count variable. 
 
 The SQL query against mySQL "information_schema" database's "tables" Table and check if count for table named "dummy". 
 
 
 Here is another example, return Long value
```
    val qm = QueryManager(open = getConnection )

    val result = qm.selectQuery(sql"select count(*) from information_schema.tables ").toSingle[Long]
```
 
####Return a list of values

```
      val parsedSql = sql"select table_name from information_schema.tables"
      val r = qm.selectQuery(parsedSql).toList[String]
```

toList() will return a list of queried values. Here above examples returns all the table names from the mySQL information_schema.tables. 

####Return a list of tuples
```
        val qm = QueryManager(open = getMySQLConnection)
        val tuple2Value = qm.selectQuery(sql" select 1, '2' from dual").toSingle[(Int, String)]
        assert(tuple2Value.get === (1, "2"))

        val date = new Date()
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val tuple3Value= qm.selectQuery(sql"select 1.0, '2', now() from dual").toSingle[(Double, String, Date)]
        assert(tuple3Value != None)

        val (x,y,z) = tuple3Value.get
        assert((x,y) === (1.0, "2"))
        assert( formatter.format(z) === formatter.format(date))

        val tuple4Value = qm.selectQuery(sql" select 1,2,3,4  from dual").toSingle[(Int,Int, Int,Int   )]
        assert(tuple4Value.get === ( 1,2,3,4 ))

        val tuple5Value = qm.selectQuery(sql" select 1,2,3,4,5   from dual").toSingle[(Int, Int, Int,Int, Int )]
        assert(tuple5Value.get === ( 1,2,3,4,5  ))

        val tuple6Value = qm.selectQuery(sql" select 1,2,3,4,5,6  from dual").toSingle[(Int, Int, Int,Int, Int, Int  )]
        assert(tuple6Value.get === ( 1,2,3,4,5,6))

        val tuple7Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7  from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int )]
        assert(tuple7Value.get === ( 1,2,3,4,5,6,7))

        val tuple8Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8  from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int )]
        assert(tuple8Value.get === ( 1,2,3,4,5,6,7,8 ))

        val tuple9Value = qm.selectQuery(sql" select 1,2,3,4,5,6,7,8,9 from dual").toSingle[(Int, Int, Int,Int, Int, Int, Int, Int, Int )]
        assert(tuple9Value.get === ( 1,2,3,4,5,6,7,8,9))
```

up-to-9 tuples are built-in with SPA. I think if there are more than 9, you could consider using a customized class structure. 
   


Customized classes
</h4>

   In most cases, we would like to return not just String, Long and double, but our
   own classes structures. In OR world, this would mean mapping the table to class object etc.

   In SPA there is no such table to object mapping. But we do need to let the SPA know how the
   our field/method/parameter of our class related to certain column name (or label) in the query.

   so we can write something like this:

   val results:List[TableMetadata] = qm.queryWithClass(sqlString,classOf[TableMetadata]).toList()

   Since we don't require you specify the table, you can actually write a join query from multiple
   tables and result in one class object

  For example
  <pre><code>
  class Dummy () {
    @BeanProperty var x:String = null
    @BeanProperty var y:String = null
  }

  val sql = "select a.x as x, b.y as y from table_a a, table_b b where a.id = b.id"
  val results:List[TableMetadata] = qm.queryWithClass(sqlString,classOf[Dummy]).toList()

   </code></pre>
  
  Spa has the following ways to mapping column name (or Column label) to the class methods or parameters.

  <ul>
    <li>No mapping is needed for single column result Set</li>
    <li>Use @BeanProperty annotation</li>
    <li>Use @Column annotation</li>
  </ul>

  Java non-static Inner class and Scala inner class not declared in the companion object are not supported.

<h5>
  Single Column Results
<h5>

  Example you see so far are mostly single column of values such String, Long etc.
  But we can also use it for Java/Scala Objects. For example:

  Suppose we have a object that's serializable,

  <pre><code>
  class SerializedObj(x: String, z:Long) extends Serializable {
    override def toString: String = "(" + x + "," + z + ")"
  }
  </code></pre>

  and we decide to insert the object as blob into database.

  The table is something like this:

  create table if not exists test2(x BLOB)

  Then we can query the blob by the following:
  <pre><code>

  val sql = " select x from test2 limit 1"
  val value = qm.queryWithClass(sql, classOf[SerializedObj]).toSingle()
  
  </code></pre>

  There is no mapping needed.
  
<h5>
  Use @BeanProperty annotation
</h5>

  For example, we have class capture the metadata of the database table:
  
  <pre><code>
  
  class TableMetadata () {
    @BeanProperty var schema:String = null
    @BeanProperty var name:String = null
    @BeanProperty var tableType:String = null
    @BeanProperty var createTime: Date = null
    @BeanProperty var checksum: Long = -1
  }
  </code></pre>
  
  The class has a default constructor and setter methods (<field>_$eq) generated by compiler.
   
   <pre><code>
   def testSelectWithBeanProperties() {

      val qm = QueryManager(open = getConnection)
      val sqlString = " select table_schema as 'schema' , " +
                      "        table_name as 'name', " +
                      "        table_type as 'tableType', " +
                      "        create_time as 'createTime', " +
                      "        ifNull(checksum, -1)  as 'checksum' " +
                      " from information_schema.tables limit 10 "

      val results = qm.queryWithClass(sqlString,classOf[QueryTest.TableMetadata]).toList()
      assert (results.size == 10)
    }
   </code></pre>

    In this approach, we need to write SQL in such way that return column label or column name match
    to the field name.

    for example:

        column name    column label    field
        ------------------------------------
        table_schema   schema          schema
        table_name     name            name
        ...


  While if you don't want to re-write SQL or simply want to use "select * from ...", when we need to
  map column name directly using Column annotation.
<h5>
  Use @Column Annotation
</h5>  

  1) Annotation the fields with default constructor

<pre><code>

  class TableMetadata2 () {
    @Column("table_schema") var schema:String = null
    @Column("table_name")  var name:String = null
    @Column("table_type")  var tableType:String = null
    @Column("create_time")  var createTime: Date = null
    @Column("checksum")  var checksum: Long = -1
  }
 </code></pre>
  Same example as @BeanProperty, we replace the annotation to @Column annotation.

  we can now write the code as follows

  <pre><code>
  val sql = " select table_schema, table_name, table_type, create_time, ifNull(checksum, -1) as 'checksum' " +
            " from information_schema.tables limit 10"

  val results = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata2] ).toList()

  </code></pre>
  The only thing we have deal with is the null value on checksum,

  ifNull(checksum, -1) as 'checksum'


Similarly, we can annotation the class in other ways:

  here we use default constructor, but instead annotation the field, we put @Column on setter methods
  for the fields (setSchema, setName), here If the setter method is actually in the format
  of set<Fieldname>, we don't really need the annotation, as Spa will process it by convention.

  If you do use @Column on the setter method, the name of the method doesn't really matter. For example
  the method setSchema could be _mySchema, youSchema, schemaSetter ...
<pre><code>
  class TableMetadata4 () {

    var schema:String = null
    @Column("table_schema")
    def setSchema(s : String) = this.schema = s

    var name:String = null
    @Column("table_name")
    def setName(s: String)  = this.name = s

    @Column("table_type") var tableType:String = null
    @Column("create_time")  var createTime: Date = null
    @Column("checksum")  var checksum: Long = -1
  }
  </code></pre>

 Or without default constructor, and annotate the parameters of the constructor.
<pre><code>
  class TableMetadata6 (@Column("table_schema") val schema:String,
                        @Column("table_name") val name:String,
                        @Column("table_type") val tableType: String)    {

    @Column("create_time") var createTime: Date = null
    @Column("checksum")   var  checksum: Long = -1
  }
</code></pre>

<h4>
Query with Iterator
</h4>

  Instead of using Query.toList(), which put every into memory, we can also using
  Query.withIterator() and allows one to process one row at time.
  here is an example:
<pre><code>
  def testSelectWithIterator() {

    val qm = QueryManager(open = getConnection)
    val sqlString = ...
    val q = qm.queryWithClass(sqlString, classOf[QueryTest.TableMetadata6] )

    q.withIterator { it =>
      var r = it.next()
      while (r != None) {

        //process results r.get
        ...

        r = it.next()
      }
    }
  }
</code></pre>
<h4>
 Provide your own way of row extraction.
</h4>

 So far, we only allows one way to processing the result set, that is by providing the
 class of the object. This may be ok for most of use cases, but there are cases that you
 would like you own way of extracting the row. This could be that you don't want to define
 a class just for get data out of database.

 Spa allows you to do this by providing RowExtractor, there is one example:

 In this example, I want to extract the row to be a pair tuple (x, y) instead of
 a class.
<pre><code>
      def testRowExtractor() {
        val qm = QueryManager(open = getConnection)
        val rowProcessor = new RowExtractor[(String, String)] {
          def extractRow(oneRow: Map[ColumnMetadata, Any]): (String, String) = {
            assert(oneRow.size ==2 )
            val schema = oneRow.filter(c => c._1.colLabel == "table_schema").head._2.toString
            val name   = oneRow.filter(c => c._1.colLabel == "table_name").head._2.toString
            (schema, name)
          }
        }

        val results = qm.query(" select table_schema, table_name" +
                               " from information_schema.tables limit 10", rowProcessor).toList()
        assert(results.size == 10)
      }

</code></pre>
<h4>
 update query with "queryForUpdate"
</h4>

   Here is an example using update query to create a database
<pre><code>
     val createTableSql = "create table if not exists mytest.test(x Integer)"

     qm.queryForUpdate(createTableSql).executeUpdate

</code></pre>
<h4>
 Transaction
</h4>
   In many applications, we need to perform multiple update/deletes. And we would like
   all the updates commit to be atomic: all or nothing.

   To do this, SPA QueryManager use Transaction method, here is an example
<pre><code>
   val qm = QueryManager(open = getConnection)

   qm.transaction() { transaction =>

      qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
         .parameterByName("x", 1).executeUpdate

       qm.queryForUpdate("insert into mytest.test(x) values (:x)", transaction)
         .parameterByName("x", 2).executeUpdate
     }

</code></pre>
   notice that different from earlier update example, the queryForUpdate method now takes
   additional argument transaction; and all the udpates are within the closure of
   a transaction method.

<h4>
 Batch Update
 </h4>

   Batch Update requires a Transaction,

<pre><code>
    val InsertSql = "insert into mytest.test(x) values (:x)"
    qm.transaction() { trans =>
       val q = qm.queryForUpdate(InsertSql, trans)
       for (i <- 1 to 200 ) {
               q.parameterByName("x", i)
               q.addBatch()
       }
       q.executeBatchUpdate
    }
</code></pre>

<h4>
Logging
</h4>

   SQL logging

   When query failed due to SQL error, the Spa will print out the SQL you wrote with parameters
   and the parsed sql which sent to database.

   You can choose to print this information out even there is no error especially during development
   and testing stage. You can do this by  set Query.logSql(true)
<pre><code>
   val q = qm.queryForUpdate(InsertSql, trans)
             .logSql(true)
             .executeUpdate
</code></pre>
   Connection leaking track logging

   QueryManager has two argument
<pre><code>

   object QueryManager {

    def apply(open: => Connection, logConnection:Boolean= false): QueryManager = {
        new QueryManager(open, logConnection)
    }
  }
</code></pre>  

  When logConnection = true, the Spa will printout when the jdbc connection is opened and closed.

<h4>Decimal Precision and Scale </h4>

 If sql type is Decimal or Numeric, the scale == 0, and precision < 9  return Int

 If sql type is Decimal or Numeric, the scale == 0, and precision <= 18 and precision > 9  return Long

 If sql type is Decimal or Numeric, the scale == 0, and precision > 18   return BigDecimal

 If sql type is Decimal or Numeric, the scale > 0, and precision <= 9 return Double

 If sql type is Decimal or Numeric, the scale > 0, and precision > 9 return BigDecimal


<h3> Known Issue </h3>

     
     
     
## Changes from 0.1 version

* Major API changes: the setParameterByName(), setParameterByPos() are removed. These mutable APIs are replaced with immutable APIs
  The QueryManager's API also changed querys into selectQuery, UpdateQuery, BatchUpdateQuiers  

* Implementation Changes: try to remove all the mutable variables if possible. There are still some mutable variable in BatchQuery

* QueryIterator is replaced by Scala Iterator trait, so we can leverage all power of scala.

* The SPA now has built-in support for tuple results. 

* The customized class, the annotations on Field, and setter methods are removed to discourage mutable field/method. The SPA 2.0 now 
only supports constructor parameter annotation. 

* Named argument feature is removed. This is consequences of the removing mutable variables. For most of the query, this is not needed anymore. 
  but for batch query, this is going backward. But I think this is still a good trade off. 



