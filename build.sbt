name := "airplane_delay"

version := "0.1"

scalaVersion := "2.12.15"
idePackagePrefix := Some("es.upm.airplane")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.30.0"