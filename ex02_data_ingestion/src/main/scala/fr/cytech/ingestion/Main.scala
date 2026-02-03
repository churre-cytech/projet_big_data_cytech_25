package fr.cytech.ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions

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
        // dfClean.printSchema()
        // dfClean.show(20)

        // Priting .count() for each DataFrame to catch len diff
        // println("df :" + df.count())
        // println("dfClean :" + dfClean.count())

        // Printing only PULocationID and DOLocationID 
        // to check values for construct and load dim_location
        // dfClean.select("PULocationID", "DOLocationID")
        //     .show(20, truncate = false)

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

        // ############### Constructing and loading missing dimensions
        // ############### -- dim_date
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


        // ############### -- dim_location
        // ### Printing dim_location table for test
        // val dimLocation_table = spark.read
        //     .jdbc(jdbcUrl, "dim_location", jdbcProperties)
        // dimLocation_table.show(truncate = false)
        
        // ###### Loading taxi_zone_lookup.csv to populate dim_location columns (borough, zone, service_zone) 
        // -> (extend the analysis)
        val taxiZoneLookupPath: String = "src/main/resources/taxi_zone_lookup.csv"
        val dfTaxiZoneLookupRaw: DataFrame = spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(taxiZoneLookupPath)
        // dfTaxiZoneLookupRaw.show(20, truncate = false)
        // dfTaxiZoneLookupRaw.printSchema()
        
        val dftaxiZoneLookupClean: DataFrame = dfTaxiZoneLookupRaw
            .withColumnRenamed("LocationID", "location_id") // withColumnRenamed() only to rename column
            .withColumn("borough", trim(col("Borough"))) // withColumn() add/create value + trim() to avoid duplicate 
            .withColumn("zone", trim(col("Zone")))
            .withColumn("service_zone", trim(col("service_zone")))
            .select("location_id", "borough", "zone", "service_zone") // Keep only columns needed for dim_location
        // dftaxiZoneLookupClean.printSchema()
        // dftaxiZoneLookupClean.show(20, truncate = false)

        val pickupLocationID = dfClean.select("PULocationID").withColumnRenamed("PULocationID", "location_id")
        val dropoffLocationID = dfClean.select("DOLocationID").withColumnRenamed("DOLocationID", "location_id")

        // Previous approach : build dim_location using only PULocationID/DOLocationID (no TLC enrichment) 
        // val dimLocation = pickupLocationID.union(dropoffLocationID)
        //     .filter(col("location_id").isNotNull)
        //     .distinct()
        // dimLocation.printSchema() // To check if location_id = "integer"
        // dimLocation.show(20, truncate = false)
        // println(dimLocation.count())

        val locationIds = pickupLocationID.union(dropoffLocationID)
            .filter(col("location_id").isNotNull)
            .distinct()

        val dimLocationEnriched = locationIds
            .join(dftaxiZoneLookupClean, Seq("location_id"), "left")
        // dimLocationEnriched.show(20, truncate = false)

        dimLocationEnriched.write
            .mode("append")
            .jdbc(jdbcUrl, "dim_location", jdbcProperties) 


        // ############### -- fact_trip
        // Printing fact_trip table
        // val factTrip_table = spark.read
        //     .jdbc(jdbcUrl, "fact_trip", jdbcProperties)
        // factTrip_table.show()

        val dfFactTripBase: DataFrame = dfClean
            .withColumn("pickup_date_id", date_format(col("tpep_pickup_datetime"), "yyyyMMdd").cast("int")) 
            .withColumn("dropoff_date_id", date_format(col("tpep_dropoff_datetime"), "yyyyMMdd").cast("int"))
        // dfFactTripBase.printSchema()
        // dfFactTripBase.show(20, truncate = false)

        val dfFactTrip = dfFactTripBase.select(
            col("VendorID").cast("short").as("vendor_id"),
            col("RatecodeID").cast("short").as("ratecode_id"),
            col("payment_type").cast("short").as("payment_type_id"),
            col("store_and_fwd_flag"),
            col("PULocationID").cast("int").as("pickup_location_id"),
            col("DOLocationID").cast("int").as("dropoff_location_id"),
            col("pickup_date_id").cast("int"),
            col("dropoff_date_id").cast("int"),
            col("tpep_pickup_datetime").as("pickup_datetime"),
            col("tpep_dropoff_datetime").as("dropoff_datetime"),
            col("passenger_count"),
            col("trip_distance"),
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            col("improvement_surcharge"),
            col("congestion_surcharge"),
            col("Airport_fee").as("airport_fee"),
            col("cbd_congestion_fee"),
            col("total_amount")
        )
        // dfFactTrip.printSchema()
        // dfFactTrip.show(20, truncate = false)

        dfFactTrip.write
            .mode("append")
            .jdbc(jdbcUrl, "fact_trip", jdbcProperties)  


        spark.stop()

    }
}