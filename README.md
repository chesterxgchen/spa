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


## Version 

Current version is 0.2.0


## Build

you can checkout from master branch and build with sbt.

```
sbt package

```


## License

The project is under Apache License


## Requirements

The 0.2 version requires scala 2.10. The Scala-2.10 features such as string interpolation, TypeTag, ClassTag and implicit class are used. 


## Tested Databases 

*  MySQL 5.1.66 on Debian squeeze (all tests except for transaction test passed)

*  MySQL 5.6.13  on osx10.8 (x86_64) on MacBook Pro (all tests, including transaction test, passed)

*  Postgres SQL 8.4 on Debain sequeze (all tests, including transaction test, passed)

*  Postgres SQL 9.3.0 on osx10.8 (x86_64) on MacBook Pro (all tests, including transaction test, passed)


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

toList() will return a list of queried values. The above example returns all tables' names from the mySQL information_schema.tables. 

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
   


#### Return class structure

   In most cases, we would like to return not just simple classes (such as Int, Long, String, Date, double etc.), 
   but our own class structures. In other words, this would mean mapping the result to class.
   
   One important difference from the OR mapping, we are not mapping necessarily Table to Class. but result to class. 
   this means the return result set could be from one table or a join of many tables. There is no concept of the object relations in 
   spa.  Once the result column name or alias (column label) is mapped to class variable (detail below), we can use the query in the following way. 

```   
    val results = qm.selectQuery(sql" select * from mytest.COFFEES ")
                     .toList[Coffee]
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

Here is the complete usage (using the Coffee example from Slick) : 

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
Compiler can found out the type T is List[Coffee] and A is Option[Coffee] type. Above example can also written
as

```
  val results =  q.withIterator[List[Coffee], Option[Coffee]] { it:  =>
        it.foldLeft(List[Coffee]())( (acc, a) => a.get :: acc).reverse
      }
      
```
but we can omit the certain type information, and let compiler figure it out. 


```
  val results =  q.withIterator { it: Iterator[Option[Coffee]]  =>
        it.foldLeft(List[Coffee]())( (acc, a) => a.get :: acc).reverse
      }
```
  
Here the compiler can figure the resulting T is List[Coffee]; but for compiler to know the Iterator is Iterator[Option[Coffee]]
we need to indicate it's type : Iterator[Option[Coffee]] 

 
#### Provide your own way of row extraction.


 So far, we only allows one way to processing the result set, i.e. use built-in class extractor. This may be ok for most of use cases, 
 but there are cases that you would like you own way of extracting the row. Spa allows you to do this by providing RowExtractor, here is an example:
 
 assume we have a class for CoffeePrice, notice the class doesn't has the Column Annotation. As I am doing my own RowExtraction, I don't need to Column annotation.
 
```
      case class CoffeePrice( name:  String,
                              price: Double)
                        
      val qm = QueryManager(open = getMySQLConnection)

      val coffees = prepareCoffee(qm)

      val rowProcessor = new RowExtractor[CoffeePrice] {

        def extractRow(oneRowCols: Map[ColumnMetadata, Any]): CoffeePrice = {

          import scala.math.BigDecimal.javaBigDecimal2bigDecimal

          //there should only two columns
          assert(oneRowCols.size ==2 )
          val colValues = oneRowCols.map(a => a._1.colLabel -> a._2)
          val name = colValues.get("COF_NAME").getOrElse("NA").toString
          val price = colValues.get("PRICE").getOrElse(0).asInstanceOf[java.math.BigDecimal].toDouble

          CoffeePrice(name, price)
        }
      }
  
      val results = qm.selectQuery(sql" select COF_NAME, PRICE from mytest.COFFEES ", Some(rowProcessor)).toList[CoffeePrice]

```

### UpdateQuery 

   Here is an example using update query to create a database

```
     val createTableSql = sql"create table if not exists mytest.test(x Integer)"
     qm.updateQuery(createTableSql).executeUpdate

```

All create, delete, update and insert queries can use UpdateQuery. For Insert queries, the updateQuery() return the auto generated key
values; other type of update queries (create, delete, update), it returns the execution return codes.  

The auto generated keys must be numeric value, the SPA returns it as long. For MySQL this is the auto_increment number. 

here is an example, using mySQL: 

```
     qm.transaction() { implicit trans  =>
        val dropDbSql = sql"drop database if exists mytest"
        qm.updateQuery(dropDbSql).executeUpdate

        val createDbSql = sql"create database if not exists mytest"
        qm.updateQuery(createDbSql).executeUpdate

        val createTableSql = sql"create table if not exists mytest.test( id  MEDIUMINT NOT NULL AUTO_INCREMENT primary key,  x Integer)"
        qm.updateQuery(createTableSql).executeUpdate

      }

      val id1 = qm.updateQuery(sql" INSERT INTO mytest.test(x) values (1) ").executeUpdate
      assert(id1 == 1)
      val id2 = qm.updateQuery(sql" INSERT INTO mytest.test(x) values (2) ").executeUpdate
      assert(id2 == 2)

```


### Batch Update

JDBC drivers offers batch update capability, which allows one to add several set of the parameters with the same SQL statement. 
    
Here is one example, I have pre-created a database called "mytest" and a table on mytest (x Integer, y Integer). 
Instead of insert one row with one update query, I used the same statement, but each time with different set of the query parameters. 
and execute the batch update at the end.  Here the each batch parameter is stored into a Map() with key as the index of the position,
0-based. 

```
 
       val qm = QueryManager(open = getMySQLConnection)

       val q = qm.batchUpdateQuery(sql"insert into mytest.test (x, y) values(?, ?) ")

       //index is zero-based
       val size = 10
       for (i <- 0 until size ) {
         val pa = Map(0 -> i, 1 -> i*20) // x  is 0, y  is 1
         q.addBatch(pa)
       }
       
       q.executeBatchUpdate
```

### Transaction
 
   In many applications, we need to perform multiple select/update/deletes. And we would like
   all the updates commit to be atomic: all or nothing.
   
   By default, the UpdateQuery and BatchUpdateQuery will be in one transaction. If none, new transaction will be created and used. 
   
   Here is how to use explicit transaction: 
   
```
     qm.transaction() { implicit trans  =>
    
         val dropDbSql = sql"drop database if exists mytest"
         qm.updateQuery(dropDbSql).executeUpdate

         val createDbSql = sql"create database if not exists mytest"
         qm.updateQuery(createDbSql).executeUpdate
    }
```

Here is a test (using ScalaTest) for transaction rollback:    

```
       //first we drop and recreate a database table. 
       
       val dropTableSql = sql"drop table if exists mytest.test"
       qm.updateQuery(dropTableSql).executeUpdate

       val createTableSql = sql"create table mytest.test(x Integer)"
       qm.updateQuery(createTableSql).executeUpdate


       // then we insert the value 1 into the newly created table
       qm.updateQuery(sql"insert into mytest.test(x) values (1) ").executeUpdate

       //verify the value 1 is in database       
       val value = qm.selectQuery(sql"select x from mytest.test").logSql(true).toSingle[Long]
       assert(value == Some(1))


       //update the value to 2 in the table, 
       //after update, throw exception within the same transaction. 
       // the exception should cause transaction rollback
       // the database table should still have the value before transaction : i.e. 1
       
       intercept[ExecuteQueryException] {
         qm.transaction() { implicit trans =>

           qm.updateQuery(sql"update mytest.test set x = 2").executeUpdate

           throw new ExecuteQueryException("see if I can rollback")
         }
       }

       //verify that the database table still have value 1
       
       val value2 = qm.selectQuery(sql"select x from mytest.test").toSingle[Long]
       assert(value2 === Some(1))
       
       // now, repeat the update again, this time, without throw exception. 
       // the value should be updated after transaction is committed. And the value should be
       // the newly updated value : 2
       
       qm.transaction() { implicit trans =>
        qm.updateQuery(sql"update mytest.test set x = 2").executeUpdate
       }

       //verify the new value is 2 
       
       val value3 = qm.selectQuery(sql"select x from mytest.test").toSingle[Long]
       assert(value3 === Some(2))

     }

       
```


For select query only, we might not need a transaction, but when we have mix of select and update query, for example
 
```
   select
   update
   select
   delete
   select
   update
```
We don't want to start a new connection and close the new connection for every query (select or update), we would like them
all use the same connection under one transaction. Therefore, even during select query we are also using a transaction.
as result, we don't immediate close the connection after the select query, rather the connection is closed by Connection closure
which is part of the transaction, after the transaction is committed or roll back. all the queries within the same transacation 
shared the same connection.


 
### Logging
 
   SQL logging

   When query failed due to SQL error, the Spa will print out the SQL you wrote with parameters
   and the parsed sql which sent to database.

   You can choose to print this information out even there is no error especially during development
   and testing stage. You can do this by  set Query.logSql(true)
   
```
   val q = qm.updateQuery(....)
             .logSql(true)
             .executeUpdate
```
### Other Topics


#### Connection leaking track logging

   QueryManager has two arguments
   
```
   object QueryManager {

     def apply(open: => Option[Connection], logConnection:Boolean= false): QueryManager = {
         new QueryManager(open, logConnection)
     }

  }
  
``` 

  When logConnection = true, the Spa will printout when the jdbc connection is opened and closed.
  
#### Break long SQL statement into separate lines: 

  When the SQL statement is long, you might want to break it into multiple-line for easy to read. For example,

``` 
    val sql = " select count(*) from information_schema.tables where table_schema = ? and table_name = ? "
               
```

I would like to break it into : 

```
    val sql = " select count(*) from information_schema.tables " + 
              " where table_schema = ? and table_name = ? "

```

  
  With SPA, we are using a String interpolation with sql"..." syntax, breaking the statements will end up like this: 
  
```
      
       val db="mytest"
       val table="test"
       val selectTableSql = sql" select count(*) from information_schema.tables where " +
                            sql" table_schema = $db and table_name = $table"

```
 Notice that we are using 
 
 ```
                     sql"..." +
                     sql"..." 
                     
 ```

#### Decimal Precision and Scale 
 
 The followings the patterns to match in determine the simple value data type to return. The patterns matched from 
 top to bottom, if the top one is matched, it will not search to the bottom ones: 
 
 
 If sql type is Decimal or Numeric, the scale == 0, and precision  == 0  return Double. 

 If sql type is Decimal or Numeric, the scale == 0, and precision < 9  return Int

 If sql type is Decimal or Numeric, the scale == 0, and precision <= 18 and precision > 9  return Long

 If sql type is Decimal or Numeric, the scale == 0, and precision > 18   return BigDecimal

 If sql type is Decimal or Numeric, the scale > 0, and precision <= 9 return Double

 If sql type is Decimal or Numeric, the scale > 0, and precision > 9 return BigDecimal



## Known Issues and Test Requirements

* only support mySQL and postgres SQL at the moment. More tests needed for other databases (oracle, sql server and derby etc.)


* To run the test, you  will need to setup mySQL and Postgres SQL and provide the login and password for the database. The configuration file is with typesafe config
  and can be changed in 

```
   src/test/resources/application.properties
```

* I expect only you will only run one of the databases, so change the above property file database vendor to the one you want to test:

for example, the following, set the database to postgres, which means the database connection will load from application.properties
to point to postgres SQL database connection properties. The db.vendor property only affects SPA tests,
it is not used by SPA main source code.

```
# database vendor, specify the proper value to test the corresponding
# database.
# the values are
# mysql,  postgres

db.vendor= postgres

```

* To run the tests, you will need to install SBT to compile, package and test

here is how to run the tests (written in ScalaTest)

```
sbt test

```

* Scala 2.10.0 vs. 2.10.2

If you change the Scala version from 2.10.0 to 2.10.2, in build.sbt
```
scalaVersion  := "2.10.2"

...

  "org.scala-lang"            % "scala-reflect"          % "2.10.2"


```

you will get the following errors, when you do any sbt command.

[warn] Potentially incompatible versions of dependencies of {file:/Users/chester/projects/spa/}default-662c05:
[warn]    org.scala-lang: 2.10.2, 2.10.0

This is mainly related to the "scala-relfect" version. For example, change scala-reflect to 2.10.0 but keeps scalaVersion
to Scala-2.10.2

```
scalaVersion  := "2.10.2"

...

  "org.scala-lang"            % "scala-reflect"          % "2.10.0"

```

This works fine as well. So this is what we will keep.


## Things on my mind for SPA: 

  * Using macro to replace java reflection
  * change throw exception to Scala error handling with try .. sucecss/failure
  * asynchornize JDBC call
  * publish the artifact in maven repository
  * 

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

* Ant and Ivy build scripts are removed. From version 0.2.0 on, only sbt build will be used

* TestNG is replaced with ScalaTest



