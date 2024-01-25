
package main.scala.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._


/**
  * Usage: cryptoanalyse <mnm_file_dataset>
  */
object cryptoanalysejob {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("cryptoanalyse")
      .getOrCreate()

     // Vérifiez que le chemin du fichier JSON est fourni en argument
    if (args.length < 1) {
      println("Usage: CryptoAnalysisJob <json_file>")
      sys.exit(1)
    }

    // Récupérez le chemin du fichier JSON d'entrée
    val jsonFile = args(0)

  // Définissez manuellement le schéma en fonction de la structure réelle
    val customSchema ={ StructType(
      Array(
        StructField("data", ArrayType(
          StructType(
            Array(
              StructField("id", LongType),
              StructField("liquidity", LongType),
              StructField("logo", StringType),
              StructField("market_cap", LongType),
              StructField("name", StringType),
              StructField("price", DoubleType),
              StructField("price_change_1h", DoubleType),
              StructField("symbol", StringType)
            )
          )
        ))
      )
    )
    }
    // Lisez le fichier JSON avec le schéma personnalisé
    val cryptoDF ={ spark.read
      .option("multiLine", true)
      .schema(customSchema)
      .json(jsonFile)}

    // Sélectionnez les colonnes nécessaires (ajustez selon la structure réelle)
    val resultDF = {cryptoDF.select(
      "data.id",
      "data.liquidity",
      "data.logo",
      "data.market_cap",
      "data.name",
      "data.price",
      "data.price_change_1h",
      "data.symbol"
    )}

    // Affichez le DataFrame résultant
    resultDF.show(1)

    // Obtenez la date du jour au format "dd-MM-yyyy"
    val currentDate = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("dd-MM-yyyy"))

    // Obtenez l'heure actuelle au format "HH"
    val currentHour = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH"))

 

    // Construisez le chemin complet avec la date et l'heure actuelles
    val hdfsPath = s"hdfs://localhost:9000/home/hadoop/hdfs/namenode/mobula/formatted/$currentDate/$currentHour"

    // Écrivez le DataFrame au format Parquet dans le répertoire "formatted"
    resultDF.write.mode("overwrite").parquet(hdfsPath)
  }
}