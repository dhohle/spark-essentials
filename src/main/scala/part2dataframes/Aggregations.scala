package part2dataframes

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val majorGenre = "Major_Genre";

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val movieDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //  movieDF.show()

  // Counting
  // counting all (excluding null)
  movieDF.select(count(col(majorGenre)))//.show()
  movieDF.selectExpr("count(Major_Genre)")//.show()
  // counting all (including null)
  movieDF.select(count("*"))//.show()

  // counting distinct values
  movieDF.select(countDistinct(col("Major_Genre")))//.show()

  // Approximate count // for very large datasets
  movieDF.select(approx_count_distinct(col("Major_Genre")))//.show()

  // min and max
  movieDF.select(min(col("IMDB_Rating")))//.show()
  movieDF.selectExpr("min(IMDB_Rating)")//.show()

  // sum
  movieDF.select(sum(col("US_Gross")))//.show()
  movieDF.selectExpr("sum(US_Gross)")//.show()

  // avg
  movieDF.select(avg(col("Rotten_Tomatoes_Rating")))//.show()
  movieDF.selectExpr("avg(Rotten_Tomatoes_Rating)")//.show()

  // data science
  movieDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")),
  )//.show()

  // Grouping
  movieDF.groupBy(majorGenre)
    .count() // select count(*) from moviesDF group by Major_Genre
//    .show()

  movieDF.groupBy(col(majorGenre))
    .avg("IMDB_Rating")

  // aggregations by genre
  movieDF.groupBy(col(majorGenre))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
    //.show()


  /**
    * Exercises:
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and std of US gross revenue for the movies
    * 4. compute avg IMDB rating and the average US gross revenue PER DIRECTOR
    */

  // Exercise 1
  movieDF.select(
    sum(col("Worldwide_Gross") + col("US_Gross") + col("US_DVD_Sales")).as("Total Profits")
  ).show()
  //alt (from example)
  movieDF.select((col("US_Gross")+ col("Worldwide_Gross")+col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))//.show()

  // Exercise 2
  movieDF.select(countDistinct(col("Director"))).show()

  // Exercise 3
  movieDF.select(
    mean(col("US_Gross")).as("Mean_US_Gross"),
    stddev(col("US_Gross")).as("STD_US_Gross")
  ).show()
  // alt
  movieDF.select(mean("US_Gross"), stddev("US_Gross")).show()


  // Exercise 4
  movieDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Mean_IMDB_Rating"),
      sum("US_Gross").as("Sum_IMDB_Rating"),
      avg(columnName = "US_Gross").as("Mean_US_Gross")
  )
    .orderBy(col("Mean_IMDB_Rating").desc_nulls_last)
//    .orderBy(col("Mean_IMDB_Rating").asc_nulls_last)
    .show()











}
