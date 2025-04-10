package dc.paris.integration

import org.apache.spark.sql.SparkSession
import java.io.File

object Main extends App {
  // Initialisation de SparkSession
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "t8zVFrnzZgkxvWOSGeTO") // Clé d'accès S3
    .config("fs.s3a.secret.key", "XL41XtFcgyhytMeFDyaUEitBXztu7BGy8tGwNy4u") // Clé secrète S3
    .config("fs.s3a.endpoint", "http://localhost:9000/") // Endpoint S3
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  // Chemin absolu vers le fichier Parquet
  private val parquetFilePath = new File("../data/raw/yellow_tripdata_2024-10.parquet").getAbsolutePath
  private val parquetFile = spark.read.parquet(parquetFilePath)

  // Création d'une vue temporaire et interrogation SQL
  parquetFile.createOrReplaceTempView("parquet")
  private val test = spark.sql("SELECT * FROM parquet")
  test.show(10) // Affiche seulement les 10 premières lignes

  // Écriture des données dans un bucket S3
  private def fileUploader(): Unit = {
    val url = "s3a://spark/yellow_tripdata_2024-10v3.parquet" // URL du bucket S3 avec le nom du fichier
    parquetFile.write.mode("overwrite").parquet(url)
    println(s"Les données ont été écrites dans le bucket : $url")
  }

  // Exécution de l'upload
  fileUploader()
}