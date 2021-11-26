package es.upm.airplane
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Main extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Airlane Delay")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val schema = StructType(Array(
    StructField("Year",IntegerType,true),
    StructField("Month",IntegerType,true),
    StructField("DayofMonth",IntegerType,true),
    StructField("DayofWeek", IntegerType,true),
    StructField("Deptime",IntegerType,true),
    StructField("CRSDepTime",IntegerType,true),
    StructField("ArrTime",StringType,true),
    StructField("CRSArrTime",IntegerType,true),
    StructField("UniqueCarrier",StringType,true),
    StructField("FlightNum",IntegerType,true),
    StructField("TailNum",StringType,true),
    StructField("ActualElapsedTime",StringType,true),
    StructField("CRSElapsedTime",IntegerType,true),
    StructField("AirTime",StringType,true),
    StructField("ArrDeplay",IntegerType,true),
    StructField("DepDelay",IntegerType,true),
    StructField("Origin",StringType,true),
    StructField("Dest",StringType,true),
    StructField("Distance",IntegerType,true),
    StructField("TaxiIn",IntegerType,true),
    StructField("TaxiOUt",IntegerType,true),
    StructField("Cancelled",IntegerType,true),
    StructField("CancellationCode",StringType,true),
    StructField("Diverted",StringType,true),
    StructField("CarrierDelay",StringType,true),
    StructField("WeatherDelay",StringType,true),
    StructField("NASDelay",StringType,true),
    StructField("SecurityDelay",StringType,true),
    StructField("LateAircraftDelay",StringType,true),
  ))

  val df = spark.read.option("header",true).option("delimiter", ",").schema(schema).csv("./data/2008.csv.bz2")

  //println(df.printSchema())

  val dfRemoveColumns= df.drop("ArrTime", "ActualElapsedTime",
    "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay")

  //println(dfRemoveColumns.groupBy("Cancelled").count().show())
  //CancellationCode has a lot of empty values, caused by flights which were not cancelled.
  //println(dfRemoveColumns.groupBy("CancellationCode").count().show())
  //println(dfRemoveColumns.show(5))
}
