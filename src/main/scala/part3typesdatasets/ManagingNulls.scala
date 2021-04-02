package part3typesdatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Managing Nulls").config("spark.master", "local").getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Select the first non-null value, using coalesce
  moviesDF.select(
    col("Title"),
//    col("Rotten_Tomatoes_Rating"),
//    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")*10).as("Rating")
  )
    //.show()

  // checking for null
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls (rows where null)
  moviesDF.select("Title", "IMDB_Rating").na.drop()// remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "IMDB_Rating"))
//    .show()

  //
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating"-> 10,
    "Director"-> "Unknown"
  )).show()

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as ifnull
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating*10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating*10, 0.0) as nvl2", // if (first != null) second else third
  ).show()
}
