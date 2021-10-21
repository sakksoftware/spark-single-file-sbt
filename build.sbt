name := "spark-single-file-sbt"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.0"