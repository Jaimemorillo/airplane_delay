package es.upm.airplane
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.math.Pi

object Main extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Airlane Delay")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  // Create schema and read csv
  val schema = StructType(Array(
    StructField("Year",StringType,true),
    StructField("Month",StringType,true),
    StructField("DayofMonth",StringType,true),
    StructField("DayofWeek", IntegerType,true),
    StructField("DepTime",StringType,true),
    StructField("CRSDepTime",StringType,true),
    StructField("ArrTime",StringType,true),
    StructField("CRSArrTime",StringType,true),
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
    StructField("TaxiOut",DoubleType,true),
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
  val dfRemoveColumns= df.drop( "ArrTime","ActualElapsedTime",
    "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay",
    "SecurityDelay", "LateAircraftDelay", "CancellationCode", "Year")

  //Filter out not arriving flights
  val dfFilterArrDelay = dfRemoveColumns.filter($"ArrDelay".isNotNull)

  //Filter out cancelled flights
  val dfFilterCancelled = dfFilterArrDelay.filter($"Cancelled" === 0)
    .drop("Cancelled")

  //Fill null values TaxiOut
  val meanTaxiOUt = dfFilterArrDelay.select(mean("TaxiOut")).first().getDouble(0)
  val dfFilledTaxiOut = dfFilterCancelled.na.fill(meanTaxiOUt,Array("TaxiOut"))

  //Filter out null values
  val dfNullDropped = dfFilledTaxiOut.na.drop()

  //Merge full date (Leap-year¿?)
  val dfDate = dfNullDropped
    .withColumn("DayofMonth",
      when($"DayofMonth"===29 && $"Month"===2, 28)
        .otherwise($"DayofMonth"))
    .withColumn("DateString",
    concat(lpad($"DayofMonth", 2, "0"),
      lit("-"),
      lpad($"Month", 2, "0"),
      lit("-"),
      lit("2002")))
    .withColumn("Date", to_date($"DateString", "dd-MM-yyyy"))
    .withColumn("DayofYear", date_format($"Date", "D"))
    .drop("DayofMonth", "Month", "Year", "DateString")

  //Complete times
  val dfTimes = dfDate
    .withColumn("DepTime", lpad($"DepTime", 4, "0"))
    .withColumn("CRSDepTime", lpad($"CRSDepTime", 4, "0"))
    .withColumn("CRSArrTime", lpad($"CRSArrTime", 4, "0"))

  //Cyclical features encoding
  def cyclicalEncodingTime (dfIni: DataFrame, columnName:String) : DataFrame = {
    // Assign 0 to 00:00 until 1439 to 23:59 (= 24 hours x 60 minutes)
    // Create table with encoding
    val ini = DateTime.now().hour(0).minute(0).second(0)
    val values = (0 until 1440).map(ini.plusMinutes(_)).toList
    val hourAndMinutes = values.map(x=>(x.getHourOfDay().toString, x.getMinuteOfHour().toString))
    val columns = Seq("Hour","Min")
    val dfCyclical = hourAndMinutes.toDF(columns:_*)
      .withColumn("Time",
        concat(lpad($"Hour", 2, "0"),
          lpad($"Min", 2, "0")))
      .drop("Hour", "Min")
      .withColumn("id", monotonically_increasing_id())
      .withColumn("idNorm",
        round(lit(2)*lit(Pi)*$"id"/lit(1439),6))
      .withColumn("x" + columnName, round(cos($"idNorm"),6))
      .withColumn("y" + columnName, round(sin($"idNorm"),6))
      .drop("id", "idNorm")

    val dfOut = dfIni.join(dfCyclical, dfIni(columnName)===dfCyclical("Time"), "inner")
      .drop("Time")
    return dfOut
  }

  val dfCRSDepTimeEncoded = cyclicalEncodingTime(dfTimes, "CRSDepTime")
  val dfCRSArrTimeEncoded = cyclicalEncodingTime(dfCRSDepTimeEncoded, "CRSArrTime")
    .drop("DepTime","CRSDepTime","CRSArrTime")
  //val dfDepTimeEncoded = cyclicalEncodingTime(dfCRSArrTimeEncoded, "DepTime")

  def cyclicalEncodingDate(dfIni: DataFrame, columnName:String) : DataFrame = {
    val dfCyclical = (0 until 366).toList.toDF("Days")
      .withColumn("idNorm",
        round(lit(2)*lit(Pi)*$"Days"/lit(365),6))
      .withColumn("x" + columnName, round(cos($"idNorm"),6))
      .withColumn("y" + columnName, round(sin($"idNorm"),6))
      .drop("idNorm")

    val dfOut = dfIni.join(dfCyclical, dfIni(columnName)===dfCyclical("Days"), "inner")
      .drop("Days")
    return dfOut
  }

  val dfDateEncoded = cyclicalEncodingDate(dfCRSArrTimeEncoded, "DayofYear")
    .drop("Date", "DayofYear")

  //Encoding for categorical variables
  def meanTargetEncoding(dfIni: DataFrame, columnName:String) : DataFrame = {
    //JUST APPLY WITH TRAIN
    val dfOut = dfIni.withColumn(columnName + "Encoded",
      round(avg("ArrDelay").over(Window.partitionBy(columnName)),6))
      .select(columnName, columnName + "Encoded")
      .distinct()
    return dfOut
  }

  // Apply to train and join the resulting encoding with train, test and val
  val uniqueCarrierEncoding = meanTargetEncoding(dfDateEncoded, "UniqueCarrier")
  val DayofWeekEncoding = meanTargetEncoding(dfDateEncoded, "DayofWeek")
  val FlightNumEncoding = meanTargetEncoding(dfDateEncoded, "FlightNum")
  val TailNumEncoding = meanTargetEncoding(dfDateEncoded, "TailNum")
  val OriginEncoding = meanTargetEncoding(dfDateEncoded, "Origin")
  val DestEncoding = meanTargetEncoding(dfDateEncoded, "Dest")

  println(OriginEncoding.show)

}
