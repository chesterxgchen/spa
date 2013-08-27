organization := "com.xiaoguangchen"

name := "spa"

version := "0.2.0-SNAPSHOT"

scalaVersion  := "2.10.2"

unmanagedBase := file( "lib" ).getAbsoluteFile

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
   "maven repo " at "http://mvnrepository.com/"
)

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

//credentials += Credentials( new File("project/.ivy2/.credentials") )

fullClasspath in Test += file( "src/test/resources")



libraryDependencies ++= Seq(
   "org.scala-lang"            % "scala-reflect"          % "2.10.2"
   ,"mysql"                    % "mysql-connector-java"   % "5.1.23" % "test"
   ,"org.scalatest"            % "scalatest_2.10"         % "1.9.1" % "test"
   ,"com.typesafe.config"      % "config"                 % "0.3.0" % "test"
   ,"postgresql"               % "postgresql"             % "8.4-702.jdbc4" % "test"
)
