name := "spark-scala-examples"
version := "0.1"
scalaVersion := "2.12.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0" % "provided"

