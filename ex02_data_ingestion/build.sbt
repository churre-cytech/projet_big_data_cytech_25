name := "ex02_data_ingestion"
version := "1.0.0"
scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
)

fork := true
run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Dlog4j.configurationFile=src/main/resources/log4j2.properties"
)
