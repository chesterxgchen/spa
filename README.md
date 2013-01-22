spa
===

spa -- Scala Persistence API, a scala JDBC wrapper

Chester Chen

Spa is a JDBC wrapper to make query database easier. It is not Object-Relation mapping library.
There is no special query language, joins etc, just native SQL.

The spa with close the connection/statement/resultSet without user to worry about the leaking of resources.

The following are some of the features support by spa:

    -- named argument, we can named each argument, and one can set parameter by name.
    -- auto release resources (connection, statement, and resultSet)
    -- allows one to specify the query isolation level and fetch size
    -- populate the user defined class instance with results queried from database via
       annotations and convention.
    -- flexible ways of handling results via toList, toSingle and withIterator methods
    -- Use can also customize results processing via Customized RowExtractor
    -- support batch update
    -- automatic sql logging when query failed.
    -- support transaction  -- updates withing the transaction will be commit or rollback together.

not supported

    -- Store Procedure call

The store procedure might be added when needs arise.

not intend to support

    -- connection Pool

     
     



Background

This work is inspired by the JPA (java persistence api) native query, which has the ability to select database results by simply pass the class and native SQL.
The original JPA doesn't support batch update, iterator, transaction and other features in SPA. The transaction related feature is inspired by Querulous. The transaction construct is borrowed from there.

Use at your own risk

I have used this library for my own projects, mostly small projects. Therefore use at your own risk.


License

The project is under Apache License



Build Scripts
   The project has both sbt build and ant+ ivy build scripts. I am using this project to experiment the
   sbt build. But also want to have the Ant so I can get things done quickly.

Test Framework
   The project uses TestNG framework.

Usage Examples

The following examples will use mySQL database root user
and information_schema database.

1. Select a Single String value

<pre>

def testSelectSingle() {

   val qm = QueryManager(open = getConnection )

   val sqlString: String = "select table_name
                            from information_schema.tables
                            where table_name = ?"

   val result = qm.queryWithClass(sqlString, classOf[String])
                  .parameterByPos(1, "Tables")
                  .toSingle()

   assert(result != None)
   assert(result.get.equalsIgnoreCase("TABLES"))
 }

</pre>

In this example, the getConnection() wil return the Connection. The call-by-name argument
open is used. The QueryManager usages queryWithClass to create a query by specify SQL and
String class. the toSingle() returns the result Option[String]. The parameter is set by position.

Another example, return Long value
<pre>
 def testSelectSingleValue() {

    val qm = QueryManager(open = getConnection )

    val sqlString = "select count(*) from information_schema.tables "

    val result = qm.queryWithClass(sqlString, classOf[Long]).toSingle()

    assert(result != None)
    assert(result.get > 0)
  }

</pre>
2. Named Argument

 Look the same example in #1, if we change the SQL
<pre><code>
    val sqlString: String = " select table_name " +
                            " from information_schema.tables " +
                            " where table_name = :tableName "

    val result = qm.queryWithClass(sqlString, classOf[String])
                   .parameterByName("tableName", "Tables")
                   .toSingle()
 </code></pre>
 then we can use the named argument. The benefits of the named-argument not only
 limited to using easy to remember names to identify the argument, but also avoid
 repeated place the same argument at different positions.

 For example,  if the SQL

     select user_name from user_table
     where login = ? and exist (select id from user_priv where user_login = ?)

 I just made-up these tables to illustrate a point, so don't waste time if these design of the tables
 make sense or not.

 In this case, "login" appears in both where clause and sub-query where clause.
 so user would need to do something like this if using setParameterByPos.

    .parameterByPos(1, "mylogin")
    .parameterByPos(2, "mylogin")

 If I re-write the query with named arguments,

      select user_name from user_table
      where login = :login and exist (select id from user_priv where user_login = :login)

then I only need to call the parameterByName once:

          .parameterByName("login", "mylogin")

3. Return a list of values
    // example 1
    val results = qm.queryWithClass("show databases", classOf[String])
                    .toList()

    //example 2 with specified fetch size
    val sqlString = "select table_name from information_schema.tables limit 10, 20"
    val results = qm.queryWithClass(sqlString, classOf[String]).fetchSize(5).toList()


4. Customized classes

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


  Single Column Results

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


  Use @BeanProperty annotation

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

  Use @Column Annotation

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

 4. Query with Iterator

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

 6. Provide your own way of row extraction.

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
 7. update query with "queryForUpdate"

   Here is an example using update query to create a database
<pre><code>
     val createTableSql = "create table if not exists mytest.test(x Integer)"

     qm.queryForUpdate(createTableSql).executeUpdate

</code></pre>
 8. Transaction
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


 8. Batch Update

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
  10. Logging

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











The spa will not provide connection pool management, as connection can simply get from data source or the connection pool
managed by other libraries. Spa only concerns the JDBC query.


<h1> Known Issue </h1>

Downcastng such as converting Long value to Int value doesn't work when returning query value. 
For example, 
<pre><code> select count(*) from mytable. </code></pre> 
The count value is a Long object. But I know the number can
be represented by Int value, so I told Spa to give me the Int by passing the classOf[Int] in the query. 
But this will fail. You can work around by pass classOf[Long] and then call value.toInt 
     
     
     
