// scalastyle:off println

package main.scala.main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime}


object jobformated {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("jobformated")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }
    // get the M&M data set file name
    val csvfile = args(0)

    val currentDate = LocalDate.now().toString
    
      //lecture du fichier
    var df = spark.read.option("header", true).parquet(csvfile)

    df = df.withColumn("country",explode(split(col("countries_tags"), ",")))

    df = df.withColumn("country_", trim(regexp_replace(col("country"), "en:", "")))

    // Telecharge les images du pays cameroun
    df.filter(col("country_") === "cameroon").rdd.foreachPartition(partition => {
        partition.foreach(record => {
            val runtime = Runtime.getRuntime
            val url = record.getAs[String]("image_url")
            if(url != null){
                val code = record.getAs[Double]("code").toString()
                val command = s"/Users/juniortemgoua/Documents/projects/BigData/BigDataEc/write_image.sh $code $url"
                runtime.exec(command)
            }
        })
    })

    //Creation d'un dataset pour les produits par pays 
    val uniqueCountries = df.select("country_").distinct().collect()
     
    {uniqueCountries.foreach { row =>
        val country = row.getString(0)
        val countryDF = df.filter(col("country_") === country)
        countryDF.write.mode("overwrite").parquet(s"/Users/juniortemgoua/workspace/data/off_usage/$currentDate/pays/$country")
    }}

    //afficher le schema
    df.printSchema

  }
}
