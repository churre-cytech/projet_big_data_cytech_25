package fr.cytech.retrieval

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.net.URL
import org.apache.spark.sql.SparkSession
import io.minio.{BucketExistsArgs, MakeBucketArgs, MinioClient, PutObjectArgs}


object Main {

    // This helper creates the bucket path once before writing to MinIO.
    def ensureBucket(
        endpoint: String,
        accessKey: String,
        secretKey: String,
        bucketName: String
    ): Unit = {
        val minio = MinioClient.builder()
            .endpoint(endpoint)
            .credentials(accessKey, secretKey)
            .build()

        val exists = minio.bucketExists(
            BucketExistsArgs.builder().bucket(bucketName).build()
        )
        if (!exists) {
            minio.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build())
        }
    }


    // Download the monthly parquet directly from NYC URL and stream it to MinIO.
    // This keeps the original file as a single object in the nyc-raw bucket.
    def uploadDirectToMinio(
        sourceUrl: String,
        endpoint: String,
        accessKey: String,
        secretKey: String,
        bucketName: String,
        objectName: String
    ): Unit = {
        ensureBucket(endpoint, accessKey, secretKey, bucketName)

        val minio = MinioClient.builder()
            .endpoint(endpoint)
            .credentials(accessKey, secretKey)
            .build()

        val in = new URL(sourceUrl).openStream()
        try {
            minio.putObject(
                PutObjectArgs.builder()
                .bucket(bucketName)
                .`object`(objectName)
                .stream(in, -1, 10 * 1024 * 1024) // unknown size, multipart 10MB
                .contentType("application/octet-stream")
                .build()
            )
            println(s"Uploaded to s3://$bucketName/$objectName")
        } finally {
            in.close()
        }
    }

    // Download parquet to local disk, then read/write it with Spark to MinIO (s3a://).
    // Spark writes parquet as a dataset directory (part files + _SUCCESS), not a single file.
    def uploadViaSparkLocal(
        sourceUrl: String,
        localPath: String,
        endpoint: String,
        accessKey: String,
        secretKey: String,
        bucketName: String,
        objectName: String
    ): Unit = {
        ensureBucket(endpoint, accessKey, secretKey, bucketName)

        val path = Paths.get(localPath)
        Files.createDirectories(path.getParent)

        val in = new URL(sourceUrl).openStream()
        try {
            Files.copy(in, path, StandardCopyOption.REPLACE_EXISTING)
        } finally {
            in.close()
        }

        val spark = SparkSession.builder()
            .appName("ex01_data_retrieval")
            .master("local[*]")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", accessKey)
            .config("spark.hadoop.fs.s3a.secret.key", secretKey)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()

        try {
            val df = spark.read.parquet(localPath)
            df.write
                .mode("overwrite")
                .parquet(s"s3a://$bucketName/$objectName")
        } finally {
            spark.stop()
        }
    }


    def main(args: Array[String]): Unit = {
        val sourceUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-05.parquet"
        val endpoint  = sys.env.getOrElse("MINIO_ENDPOINT", "http://localhost:9000")
        val accessKey = sys.env.getOrElse("MINIO_ACCESS_KEY", "minio")
        val secretKey = sys.env.getOrElse("MINIO_SECRET_KEY", "minio123")
        val bucketName = "nyc-raw"
        val objectName = "yellow_tripdata_2025-05.parquet"
        val localPath = "../data/raw/yellow_tripdata_2025-05.parquet"

        // Preferred for exercise 1 step 3 : direct URL -> Minio upload
        uploadDirectToMinio(sourceUrl, endpoint, accessKey, secretKey, bucketName, objectName)
        
        // Alternative : URL -> local file -> Spark -> Minio parquet dataset
        // uploadViaSparkLocal(sourceUrl, localPath, endpoint, accessKey, secretKey, bucketName, objectName)
    }
}
