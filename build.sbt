name := "CSV to Elasticsearch"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.0.0-alpha2"
