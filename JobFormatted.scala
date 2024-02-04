import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate

object JobFormatted {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("JobFormatted")
      .getOrCreate()
    val executionDate = spark.conf.get("spark.airflow.execution_date")
    val currentDate: String = if(executionDate != null) executionDate else LocalDate.now().toString
    spark.read.json(f"hdfs://localhost:9000/datalake/raw/openweathermap/weather/$currentDate.json")
    /*
    * Write your code here
    * Just use the currentDate*/
  }
}
