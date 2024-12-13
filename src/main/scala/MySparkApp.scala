import org.apache.spark.sql.SparkSession

object MySparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    val df = spark.read
      .option("header", value = true) // Use first line as header
      .csv("data/AAPL.csv") // Path to your CSV file
    df.show()
    df.printSchema()
  }
}