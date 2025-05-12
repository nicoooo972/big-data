package dc.paris.integration

import org.apache.spark.sql.SparkSession
import java.io.File

object Main extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
    .config("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  val parquetDirectoryPath = "s3a://spark/"
  val localRawDataPath = "../data/raw/"

  println(s"Début du processus de téléversement des fichiers locaux de '$localRawDataPath' vers S3 '$parquetDirectoryPath'.")
  val localDir = new File(localRawDataPath)

  if (localDir.exists && localDir.isDirectory) {
    val localParquetFiles = localDir.listFiles().filter(file => file.isFile && file.getName.endsWith(".parquet"))

    if (localParquetFiles.isEmpty) {
      println(s"Aucun fichier Parquet trouvé dans le dossier local : $localRawDataPath pour le téléversement.")
    } else {
      println(s"Téléversement de ${localParquetFiles.length} fichiers Parquet locaux vers S3 ($parquetDirectoryPath)...")
      localParquetFiles.foreach { file =>
        val localFilePath = file.getAbsolutePath
        val fileName = file.getName
        val s3DestinationPath = s"$parquetDirectoryPath$fileName"
        println(s"Traitement et téléversement du fichier local : $localFilePath vers $s3DestinationPath")

        try {
          val localDf = spark.read.parquet(localFilePath)
          localDf.write.mode("overwrite").parquet(s3DestinationPath)
          println(s"Fichier $fileName téléversé avec succès vers $s3DestinationPath")
        } catch {
          case e: Exception =>
            println(s"Erreur lors du téléversement du fichier $fileName vers S3: ${e.getMessage}")
        }
      }
      println("Tous les fichiers Parquet locaux spécifiés ont été traités pour le téléversement vers S3.")
    }
  } else {
    println(s"Le dossier source local spécifié ('$localRawDataPath', résolu en '${localDir.getAbsolutePath}') n'existe pas ou n'est pas un dossier.")
  }
  println("Fin du processus de téléversement.")
  println("-" * 50)

  println("Arrêt de la session Spark.")
  spark.stop()
}