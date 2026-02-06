ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval"
  )

// // https://mvnrepository.com/artifact/org.apache.spark/spark-core
// libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
// // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
// // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
// libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
// libraryDependencies += "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  // "org.postgresql" % "postgresql" % "42.7.3",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
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
  // "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Dlog4j.configurationFile=src/main/resources/log4j2.properties"
)

assembly / mainClass := Some("fr.cytech.retrieval.Main")
assembly / assemblyJarName := "ex01-retrieval-assembly.jar"
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.filterDistinctLines
  case PathList("META-INF", "MANIFEST.MF") =>
    MergeStrategy.discard
  case PathList("META-INF", xs @ _*)
      if xs.exists(x => x.toLowerCase.endsWith(".sf") || x.toLowerCase.endsWith(".dsa") || x.toLowerCase.endsWith(".rsa")) =>
    MergeStrategy.discard
  case PathList("META-INF", "versions", _ @ _*) =>
    MergeStrategy.discard
  case PathList("module-info.class") =>
    MergeStrategy.discard
  case _ =>
    MergeStrategy.first
}
