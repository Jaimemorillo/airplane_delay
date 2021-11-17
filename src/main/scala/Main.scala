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

  case class Person(name: String, age: Long)
  val personSeq = Seq(Person("Matt",49),
    Person("Ray", 38),
    Person("Gill", 62),
    Person("George", 28))
  var personDF = sc.parallelize(personSeq).toDF()
  personDF.filter(col("age") < 40).select(personDF("name")).show
  //That last line is equivalent to
  personDF.createOrReplaceTempView("person")
  spark.sql("select name from person where age < 40").show

  // Add a new column to the DataFrame
  personDF = personDF.withColumn("young", personDF("age") < 40)
  val groupAge = personDF.groupBy("young")
    .agg(avg(personDF("age")).as("average_age"))
  groupAge.show
}
