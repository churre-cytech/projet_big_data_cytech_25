package fr.cytech.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

        // println("Hello, world")

        val df: DataFrame = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2025-05.parquet")
        df.printSchema()
        // df.show(20)
        

        val dfClean: DataFrame = df
            .filter(col("VendorID").isin(1, 2, 6, 7))
            .filter(col("tpep_pickup_datetime").isNotNull && col("tpep_dropoff_datetime").isNotNull)
            .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
            .filter(col("passenger_count") >= 0)
            .filter(col("trip_distance") >= 0)
            .filter(col("RatecodeID").isin(1, 2, 3, 4, 5, 6))
            .filter(col("store_and_fwd_flag").isin("Y", "N"))
            .filter(col("PULocationID").isNotNull && col("PULocationID") > 0)
            .filter(col("DOLocationID").isNotNull && col("DOLocationID") > 0)
            .filter(col("payment_type").isin(0, 1, 2, 3, 4, 5, 6))
            .filter(col("total_amount") >= 0)

        dfClean.show(20)

        // Priting .count() for each DataFrame to catch len diff
        println("df :" + df.count())
        println("dfClean :" + dfClean.count())

        // Writing file_clean.parquet to Minio
        dfClean.write
            .mode("overwrite")
            .parquet("s3a://nyc-clean/yellow_tripdata_2025-05_clean.parquet")

        spark.stop()

    }
}