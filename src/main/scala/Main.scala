package es.upm.airplane
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.ml.regression.{GBTRegressor, GeneralizedLinearRegression, LinearRegression, RandomForestRegressor}
import org.apache.spark.ml.feature.{Imputer, Normalizer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.evaluation.RegressionEvaluator

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

  //Filter out null values
  val dfNullDropped = dfFilterCancelled.na.drop("any", Seq("Month","DayofMonth","DayofWeek","DepTime","CRSDepTime",
    "CRSArrTime", "UniqueCarrier","FlightNum","TailNum","CRSElapsedTime","ArrDelay","DepDelay","Origin","Dest","Distance"))

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
  val dfDepTimeEncoded = cyclicalEncodingTime(dfCRSArrTimeEncoded, "DepTime")
    .drop("DepTime","CRSDepTime","CRSArrTime")

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

  val dfDateEncoded = cyclicalEncodingDate(dfDepTimeEncoded, "DayofYear")
    .drop("Date", "DayofYear")
    .withColumnRenamed("ArrDelay","label")

  //Encoding for categorical variables
  def meanTargetEncoding(dfIni: DataFrame, columnName:String) : DataFrame = {
    //JUST APPLY WITH TRAIN
    val dfOut = dfIni.withColumn(columnName + "Encoded",
      round(avg("label").over(Window.partitionBy(columnName)),6))
      .select(columnName, columnName + "Encoded")
      .distinct()
    return dfOut
  }

  // Split training, test and validation
  val Array(trainingIni, testIni, validationIni) = dfDateEncoded.randomSplit(Array(0.6,0.2,0.2),seed=1)

  // Apply encoding function to train and join the result with training, test and val
  val uniqueCarrierEncoding = meanTargetEncoding(trainingIni, "UniqueCarrier").cache()
  uniqueCarrierEncoding.show(false)
  val dayofWeekEncoding = meanTargetEncoding(trainingIni, "DayofWeek").cache()
  dayofWeekEncoding.show(false)
  val flightNumEncoding = meanTargetEncoding(trainingIni, "FlightNum").cache()
  flightNumEncoding.show(false)
  val tailNumEncoding = meanTargetEncoding(trainingIni, "TailNum").cache()
  tailNumEncoding.show(false)
  val originEncoding = meanTargetEncoding(trainingIni, "Origin").cache()
  originEncoding.show(false)
  val destEncoding = meanTargetEncoding(trainingIni, "Dest").cache()
  destEncoding.show(false)

  // Join with each sample
  val dfTraining = trainingIni.join(uniqueCarrierEncoding, Seq("UniqueCarrier"), "left")
    .join(dayofWeekEncoding, Seq("DayofWeek"), "left")
    .join(flightNumEncoding, Seq("FlightNum"), "left")
    .join(tailNumEncoding, Seq("TailNum"), "left")
    .join(originEncoding, Seq("Origin"), "left")
    .join(destEncoding, Seq("Dest"), "left")
    .drop("UniqueCarrier", "DayofWeek", "FlightNum", "TailNum", "Origin", "Dest")

  val dfTest = testIni.join(uniqueCarrierEncoding, Seq("UniqueCarrier"), "left")
    .join(dayofWeekEncoding, Seq("DayofWeek"), "left")
    .join(flightNumEncoding, Seq("FlightNum"), "left")
    .join(tailNumEncoding, Seq("TailNum"), "left")
    .join(originEncoding, Seq("Origin"), "left")
    .join(destEncoding, Seq("Dest"), "left")
    .drop("UniqueCarrier", "DayofWeek", "FlightNum", "TailNum", "Origin", "Dest")

  val dfValidation = validationIni.join(uniqueCarrierEncoding, Seq("UniqueCarrier"), "left")
    .join(dayofWeekEncoding, Seq("DayofWeek"), "left")
    .join(flightNumEncoding, Seq("FlightNum"), "left")
    .join(tailNumEncoding, Seq("TailNum"), "left")
    .join(originEncoding, Seq("Origin"), "left")
    .join(destEncoding, Seq("Dest"), "left")
    .drop("UniqueCarrier", "DayofWeek", "FlightNum", "TailNum", "Origin", "Dest")

  //Fill null values
  val colsNames = Array("TaxiOut", "UniqueCarrierEncoded", "DayofWeekEncoded", "FlightNumEncoded",
    "TailNumEncoded", "OriginEncoded", "DestEncoded")
  val imputer = new Imputer()
    .setInputCols(colsNames)
    .setOutputCols(colsNames.map(c => s"${c}Imputed"))
    .setStrategy("median")
  val imputerModel = imputer.fit(dfTraining)

  val trainingInputed = imputerModel.transform(dfTraining).drop("TaxiOut", "UniqueCarrierEncoded",
    "DayofWeekEncoded", "FlightNumEncoded", "TailNumEncoded", "OriginEncoded", "DestEncoded").cache()
  trainingInputed.show(false)
  val testInputed = imputerModel.transform(dfTest).drop("TaxiOut", "UniqueCarrierEncoded",
    "DayofWeekEncoded", "FlightNumEncoded", "TailNumEncoded", "OriginEncoded", "DestEncoded").cache()
  testInputed.show(false)
  val validationInputed = imputerModel.transform(dfValidation).drop("TaxiOut", "UniqueCarrierEncoded",
    "DayofWeekEncoded", "FlightNumEncoded", "TailNumEncoded", "OriginEncoded", "DestEncoded").cache()
  validationInputed.show(false)

  /////////////////////////////////////////////////
  ////////// MODEL ////////////////////////////////
  /////////////////////////////////////////////////

  val assembler = new VectorAssembler()
    .setInputCols(Array("CRSElapsedTime","DepDelay", "Distance", "TaxiOutImputed", "xCRSDepTime",
      "yCRSDepTime", "xCRSArrTime", "yCRSArrTime", "xDepTime", "yDepTime", "xDayofYear", "yDayofYear",
      "UniqueCarrierEncodedImputed", "DayofWeekEncodedImputed", "FlightNumEncodedImputed", "TailNumEncodedImputed",
      "OriginEncodedImputed", "DestEncodedImputed"))
    .setOutputCol("features")

  val trainingAs = assembler.transform(trainingInputed)
  val testAs = assembler.transform(testInputed)
  val validationAs = assembler.transform(validationInputed)

  //Apply a normalizer
  val scaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false)

  val scalerModel = scaler.fit(trainingAs)
  val training = scalerModel.transform(trainingAs)
  val test = scalerModel.transform(testAs)
  val validation = scalerModel.transform(validationAs)

  //Linear Regression
  val lr = new GeneralizedLinearRegression().setFamily("gaussian")
    .setFeaturesCol("normFeatures")
    .setLabelCol("label")

  // Fit the model
  val lrModel = lr.fit(training)

  // Print the coefficients and intercept for logistic regression
  //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  val predictions = lrModel.transform(test)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("mse")

  val accuracy = evaluator.evaluate(predictions)
  println(s"Test MSE: ${accuracy}")

}
