organization := "com.xiaoguangchen"

name := "spa"

version := "0.1.0-SNAPSHOT"

scalaVersion  := "2.10.0"

unmanagedBase := file( "lib" ).getAbsoluteFile

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
   "maven repo " at "http://mvnrepository.com/"
)

libraryDependencies += "org.testng" % "testng" % "6.8"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.23"

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"


seq(testNGSettings:_*)

testNGVersion         := "6.4"

testNGOutputDirectory := "target/testng"

testNGParameters      := Seq()

testNGSuites          := Seq("src/test/resources/testng.xml")

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials( new File("project/.ivy2/.credentials") )

fullClasspath in Test += file( "src/test/resources")
