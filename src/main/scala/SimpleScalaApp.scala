import org.apache.spark.sql.SparkSession

// [NOMBRE DE WORKERS CPU]
object SimpleScalaApp extends App{
  val spark = SparkSession.builder.master("local[2]").appName("Simple Scala App").getOrCreate()
 // val df = spark.range(100).toDF("number")
  //df.show(1001)

  // Option with infer schema make the spark to infer the type of the columns
 // val flights = spark.read.option("inferschema", "true").option("header", "true").csv("2015-summary.csv")
  //flights.show(5)

  //val flights2 = spark.read.option("header", "true").parquet("2015-summary.csv")
  //flights2.show(5)

  val taxi = spark.read.option("inferSchema","true").parquet("yellow_tripdata_2023-01.parquet")
  taxi.show(10)
}
