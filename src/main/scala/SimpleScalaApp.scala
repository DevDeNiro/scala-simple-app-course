import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// [NOMBRE DE WORKERS CPU]
object SimpleScalaApp extends App{

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder.master("local[2]").appName("Simple Scala App").getOrCreate()

 // val df = spark.range(100).toDF("number")
  //df.show(1001)

  // Option with infer schema make the spark to infer the type of the columns
 // val flights = spark.read.option("inferschema", "true").option("header", "true").csv("2015-summary.csv")
  //flights.show(5)

  //val flights2 = spark.read.option("header", "true").parquet("2015-summary.csv")
  //flights2.show(5)

  val taxi = spark.read.option("inferSchema","true").parquet("yellow_tripdata_2023-01.parquet")


  // Create a temporary view
  taxi.createOrReplaceTempView("taxi")

  // SQL query
  spark.sql("SELECT * FROM taxi").show(10)
  spark.sql("SELECT AVG(passenger_count) FROM taxi").show()
  spark.sql("SELECT SUM(trip_distance) FROM taxi").show()
  spark.sql("SELECT SUM(tip_amount) FROM taxi").show()
  // nombre de courses de taxi par heure du jour
  spark.sql("SELECT HOUR(tpep_pickup_datetime) AS hour, COUNT(*) FROM taxi GROUP BY hour").show()
  // nombre de courses de taxi par jour de la semaine
  spark.sql("SELECT DAYOFWEEK(tpep_pickup_datetime) AS day, COUNT(*) FROM taxi GROUP BY day").show()
  // montant total des courses de taxi par jour de la semaine
  spark.sql("SELECT DAYOFWEEK(tpep_pickup_datetime) AS day, SUM(total_amount) FROM taxi GROUP BY day").show()
  // taxi qui ont le même point de départ et d'arrivée
  spark.sql("SELECT COUNT(*) FROM taxi WHERE PULocationID = DOLocationID").show()
  // montant moyen des courses de taxi nombre de passagers
  spark.sql("SELECT passenger_count, AVG(total_amount) FROM taxi GROUP BY passenger_count").show()

  //val filtersTaxi = taxi.filter("fare_amount > 10")
  //filtersTaxi.show(10)
}
