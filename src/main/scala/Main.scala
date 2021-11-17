package es.upm.airplane
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Airlane Delay")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val df = spark.read.option("header",true).csv("./data/2008.csv.bz2")

  println(df.printSchema())

  val dfRemoveColumns= df.drop("ArrTime", "ActualElapsedTime",
    "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay")

  println(dfRemoveColumns.show(5))
}
