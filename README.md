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
 
#### Return a list of values

```
      val parsedSql = sql"select table_name from information_schema.tables"
      val r = qm.selectQuery(parsedSql).toList[String]
```

toList() will return a list of queried values. Here above examples returns all the table names from the mySQL information_schema.tables. 

#### Return tuples
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
   


#### return class structure

   In most cases, we would like to return not just simple classes (such as Int, Long, String, Date, double etc.), 
   but our own class structures. In other words, this would mean mapping the result to class.
   
   One important difference from the OR mapping, we are not mapping necessarily Table to Class. but result to class. 
   this means the return result set could be from one table or a join of many tables. There is no concept of the object relations in 
   spa.  Once the result column name or alias (column label) is mapped to class variable (detail below), we can use the query in the following way. 

```   
    val results = qm.selectQuery(sql" select * from mytest.COFFEES ").toList[Coffee]
```   

* In the following, we will use the Coffee table to show this mappng. 

  Here is the table structure: (using mySQL syntax)
  
```  
  create table if not exists mytest.COFFEES(COF_NAME varchar(30), 
                                            SUP_ID   INTEGER, 
                                            PRICE    DECIMAL(10,4)

  case class Coffee(@Column("COF_NAME") name:  String,
                    @Column("SUP_ID")   supId: Int,
                    @Column("PRICE")    price: Double)

```
 
The class structure needs not to be a case class, here we use case class for convenience. Here we use @Column annotation to annotate each
field in constructor. The @Column is actually a java annotation defined as : 


```
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD, java.lang.annotation.ElementType.FIELD,  ElementType.PARAMETER })
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface Column {

    java.lang.String value() default "" ;
}

```

Here is the complete usage: 

```
    val coffees = List(Coffee("Colombian", 101, 7.99),
                       Coffee("Colombian Decaf", 101, 8.99),
                       Coffee("French Roast Decaf", 49, 9.99))
                     
    // insert coffees in database ... details in update query section
    // ....

    val qm = QueryManager(open = getMySQLConnection)
    val results = qm.selectQuery(sql" select * from mytest.COFFEES ").toList[Coffee]

``` 

#### Query with Iterator


  Instead of using SelectQuery.toList(), which put every into memory, we can also using
  Query.withIterator() and allows one to process one row at time.
  here is the same example again: 
  
```

      val qm = QueryManager(open = getMySQLConnection)
      
      //create database, table and create and insert coffees
      val coffees = prepareCoffee(qm)

      val q = qm.selectQuery(sql" select * from mytest.COFFEES ")

      val results =  q.withIterator { it: Iterator[Option[Coffee]] =>
        it.foldLeft(List[Coffee]())( (acc, a) => a.get :: acc).reverse
      }

```

Notice where we leverage foldLeft accumulator to construct the result. The SelectQuery.withIterator() API is defined as : 

```
  def withIterator[T, A : ClassTag : TypeTag]( f: (Iterator[Option[A]]) => T)  : T 
  
```
  Compiler can found out the type T is List[Coffee] and A is Option[Coffee] type; we need to 
  


   

  
 
#### Provide your own way of row extraction.


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



