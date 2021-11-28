package es.upm.airplane
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Main extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Airlane Delay")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  // Create schema and read csv
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
    StructField("ArrDelay",IntegerType,true),
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

  // Remove forbidden columns and not useful ones
  val dfRemoveColumns= df.drop("ArrTime", "ActualElapsedTime",
    "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay", "CancellationCode")

  //Filter out cancelled flights
  val dfFilterCancelled = dfRemoveColumns.filter($"Cancelled" === 0)
    .drop("Cancelled")

  //Merge full date
  val dfDate = dfFilterCancelled
    .withColumn("DateString",
    concat(lpad($"DayofMonth", 2, "0"),
      lit("-"),
      lpad($"Month", 2, "0"),
      lit("-"),
      $"Year"))
    .withColumn("Date", to_date($"DateString", "dd-MM-yyyy"))
    .withColumn("DayofYear", date_format($"Date", "D"))
    .drop("DayofMonth", "Month", "Year", "DateString")

  //Leap-year¿?
  println(dfDate.where($"Date"==="2008-02-29").show())
  //println(dfDate.select(max("Date"),max("DayofYear")).show())

  //Separate times
  /*def sepTimes(dfIni: DataFrame, columnName:String): DataFrame = {
    val dfOut = dfIni.withColumn(columnName + "Hour", col(columnName)/100)
      .withColumn(columnName + "Hour", col(columnName)%100)
    return dfOut
  }*/

  //Cyclical features encoding
  def cyclicalEncodingTime (dfIni: DataFrame, columnName:String) : DataFrame = {
    // Assign 0 to 00:00 until 1439 to 23:59 (= 24 hours x 60 minutes)
    //2 * math.pi * df["x"] / 1439
    val dfOut = dfIni.withColumn("x", col(columnName))
    return dfOut
  }

  def cyclicalEncodingDate(dfIni: DataFrame, columnName:String) : DataFrame = {
    val dfOut = dfIni.withColumn("x", col(columnName))
    return dfOut
  }

}
