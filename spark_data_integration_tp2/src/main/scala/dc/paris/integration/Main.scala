package dc.paris.integration

import org.apache.spark.sql.SparkSession

//import java.io.File
import java.util.Properties



object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "eHoxMB4Zdklb5rsqfXWp") // Clé d'accès S3
    .config("fs.s3a.secret.key", "fmSPppV2WlokBEfqdKdULQ2MtyFL0U8vXtpkLr6O") // Clé secrète S3
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  Class.forName("org.postgresql.Driver")


  // spark.implicits._
  private val s3a_path = "s3a://spark/*.parquet"
  val df = spark.read.option("mergeSchema", "true").parquet(s3a_path)
  println("Fichier Parquet lu avec succès depuis S3.")

  df.show(5)

  private val jdbcUrl = "jdbc:postgresql://localhost:15432/taxi"
  private val jdbcProperties = new Properties()
  jdbcProperties.setProperty("user", "postgres")
  jdbcProperties.setProperty("password", "admin")

  private val targetTable = "yellow_tripdata"

  df.write
    .mode("overwrite")
    .jdbc(jdbcUrl, targetTable, jdbcProperties)

  println(s"Données écrites avec succès dans la table PostgreSQL : $targetTable")

  spark.stop()

}
