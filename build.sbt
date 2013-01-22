name := "spa"

version := "0.1.0-SNAPSHOT"

unmanagedBase := file( "lib" ).getAbsoluteFile

libraryDependencies += "org.testng" % "testng" % "6.8"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.22"

seq(testNGSettings:_*)

testNGVersion         := "6.4"

testNGOutputDirectory := "target/testng"

testNGParameters      := Seq()

testNGSuites          := Seq("src/test/resources/testng.xml")


publishTo := Some(Resolver.file("file",  new File( "/disk2/projects/spa/releases" )) )


credentials += Credentials( new File("project/.ivy2/.credentials") )
