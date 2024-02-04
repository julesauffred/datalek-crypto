import org.apache.spark.sql.SparkSession

import java.time.LocalDate

object JobCombined {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("JobCombined")
      .getOrCreate()
    val executionDate = spark.conf.get("spark.airflow.execution_date")
    val currentDate: String = if (executionDate != null) executionDate else LocalDate.now().toString

    /*
    * Write your code here
    * Just use the currentDate*/
  }
}
