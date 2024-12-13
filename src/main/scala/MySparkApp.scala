import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, expr}
import org.apache.spark.sql.types.StringType

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
//    df.show()
    df.printSchema()
//    can you as follow to select some columns
//    df.select("Date", "Open", "Close").show()
    val c = df("Open")
    val new_c = (c + 2.0).as("OpenAsStringPlus2")
    val columnString = c.cast(StringType).as("OpenColumnAsString")
    df.select(c, new_c, columnString).show()

    // chain filter
    df.select(c, new_c, columnString)
      .filter(new_c > 2.0)
      .filter(new_c > c)
      .filter(new_c === c) // use === and =!= to check if two column are the same
      .show()
    //

    val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampFromExpression")
    val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFromFunctions")
    df.select(timestampFromExpression, timestampFromFunctions).show()

    df.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp()").show()

    // not recommended
//    df.createTempView("df")
//    spark.sql("select * from df").show()
  }
}