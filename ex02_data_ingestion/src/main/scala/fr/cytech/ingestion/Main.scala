package fr.cytech.ingestion

import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]) = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName("ex02_data_ingestion")
            .master("local[*]")
            .config("spark.hadoop.fs.s3a.endpoint","http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key","minio") // FS S3A Access Key
            .config("spark.hadoop.fs.s3a.secret.key","minio123") // FS S3A Secret Key
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") // FS S3A connection ssl enabled
            .getOrCreate()
        import spark.implicits._

        println("Hello, world")

        val df = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2025-05.parquet")
        df.printSchema()
        df.show(5)

        spark.stop()

    }
}