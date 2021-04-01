package part3typesdatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Time_Diff", col("Today") - col("Actual_Release"))// Looks nice, as String
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // real number; date_add, date_sun
//    .show()

  // some dates are in another format
  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)
    //.show()

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1.
  moviesDF.select(col("Title"),
    when(to_date(col("Release_Date"), "dd-MMM-yy").isNotNull, to_date(col("Release_Date"), "dd-MMM-yy"))
    .when(to_date(col("Release_Date"), "yyyy-MM-dd").isNotNull, to_date(col("Release_Date"), "yyyy-MM-dd"))
    .when(to_date(col("Release_Date"), "MMMM, yyyy").isNotNull, to_date(col("Release_Date"), "MMMM, yyyy"))
    .otherwise("Unknown").as("Formatted_Date")
  )
//  where(col("Formatted_Date") === "Unknown")
//    .show()

  // 2.
  val stocksDF = spark.read.option("sep", ",").option("header", "true").option("inferSchema", "true").csv("src/main/resources/data/stocks.csv")
    .select(col("symbol"), col("price"), to_date(col("date"), "MMM dd yyyy").as("date"))

  stocksDF.printSchema()
//  stocksDF.show()

  // 2. course example
  val stocksDF2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("src/main/resources/data/stocks.csv")
  val stocksWithDatesDF = stocksDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
//  stocksWithDatesDF.show()

  //Structures
  // 1 - with col operators
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
//    .show()

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")
//    .show()

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // first element in the array
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love").as("has_love")
  )
    .filter(col("has_love"))
    .show()

//    .show()
}
