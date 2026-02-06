name := "ex02_data_ingestion"
version := "1.0.0"
scalaVersion := "2.13.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "org.postgresql" % "postgresql" % "42.7.3",
  ("io.minio" % "minio" % "8.5.12")
    .exclude("com.fasterxml.jackson.core", "jackson-databind")
    .exclude("com.fasterxml.jackson.core", "jackson-core")
    .exclude("com.fasterxml.jackson.core", "jackson-annotations")
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.15.3",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.3"
)

fork := true
run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Dlog4j.configurationFile=src/main/resources/log4j2.properties"
)
