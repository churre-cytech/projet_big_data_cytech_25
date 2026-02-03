package fr.cytech.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Main {
    def main(args: Array[String]) = {


        // ############### Initialization SparkSession ###############
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

        // Printing for test
        // println("Hello, world")

        // ############### Loading raw file.parquet ###############
        val df: DataFrame = spark.read.parquet("s3a://nyc-raw/yellow_tripdata_2025-05.parquet")
        // df.printSchema()
        // df.show(20)
        
        // ############### Branch 1 : validation/cleaning ###############
        val dfClean: DataFrame = df
            .filter(col("VendorID").isin(1, 2, 6, 7))
            .filter(col("tpep_pickup_datetime").isNotNull && col("tpep_dropoff_datetime").isNotNull)
            .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
            .filter(col("passenger_count") >= 1)
            .filter(col("trip_distance") > 0)
            .filter(col("RatecodeID").isin(1, 2, 3, 4, 5, 6))
            .filter(col("store_and_fwd_flag").isin("Y", "N"))
            .filter(col("PULocationID").isNotNull && col("PULocationID") > 0)
            .filter(col("DOLocationID").isNotNull && col("DOLocationID") > 0)
            .filter(col("payment_type").isin(0, 1, 2, 3, 4, 5, 6))
            .filter(col("total_amount") >= 0)

        // dfClean.show(20)
        // Priting .count() for each DataFrame to catch len diff
        // println("df :" + df.count())
        // println("dfClean :" + dfClean.count())

        // Writing file_clean.parquet to Minio bucket (local/nyc-clean need to exist, else: mc mb local/nyc-clean in terminal)
        dfClean.write
            .mode("overwrite")
            .parquet("s3a://nyc-clean/yellow_tripdata_2025-05_clean.parquet")


        // ############### Branch 2 : transformations Minio/Spark -> Postgres Datamart  ###############

        // Connection to Spark -> Postgres (JDBC)
        val jdbcUrl = "jdbc:postgresql://localhost:5432/bigdata_dwh"
        val jdbcProperties = new java.util.Properties()
        jdbcProperties.setProperty("user", "bigdata")
        jdbcProperties.setProperty("password", "bigdata123")
        jdbcProperties.setProperty("driver", "org.postgresql.Driver")

        // Test connection
        // val test = spark.read
        //     // .jdbc(jdbcUrl, "(SELECT * FROM dim_vendor)", jdbcProperties)
        //     // Read table directly
        //     .jdbc(jdbcUrl, "dim_vendor", jdbcProperties)
        // test.show(truncate = false)

        // Constructing and loading missing dimensions
        // -- dim_date
        // Printing dim_date table
        // val dimDate_table = spark.read
        //     .jdbc(jdbcUrl, "dim_date", jdbcProperties)
        // dimDate_table.show(truncate = false)

        val pickupDates = dfClean.select(to_date(col("tpep_pickup_datetime")).as("date_value"))
        val dropoffDates = dfClean.select(to_date(col("tpep_dropoff_datetime")).as("date_value"))

        val dimDate = pickupDates.union(dropoffDates)
            .filter(col("date_value").isNotNull)
            .distinct()
            .withColumn("date_id", date_format(col("date_value"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("date_value"))) 
            .withColumn("month", month(col("date_value"))) 
            .withColumn("day", dayofmonth(col("date_value"))) 
            .withColumn("day_of_week", dayofweek(col("date_value")))
            .select("date_id", "date_value", "year", "month", "day", "day_of_week")

        dimDate.write
            .mode("append")
            .jdbc(jdbcUrl, "dim_date", jdbcProperties)  


        // -- dim_location
        

        spark.stop()

    }
}