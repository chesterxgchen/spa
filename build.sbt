organization := "com.xiaoguangchen"

name := "spa"

version := "0.2.1"

scalaVersion  := "2.10.2"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
   "maven repo " at "http://mvnrepository.com/"
)


libraryDependencies ++= Seq(
   "org.scala-lang"            % "scala-reflect"          % "2.10.0"
   ,"mysql"                    % "mysql-connector-java"   % "5.1.23" % "test"
   ,"org.scalatest"            % "scalatest_2.10"         % "1.9.1" % "test"
   ,"com.typesafe.config"      % "config"                 % "0.3.0" % "test"
   ,"postgresql"               % "postgresql"             % "8.4-702.jdbc4" % "test"
)
