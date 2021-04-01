package part3typesdatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
  //    .show()

  // Booleans
  moviesDF.select("Title").where(col("Major_Genre") === "Drama")
  moviesDF.select("Title").where(col("Major_Genre") equalTo "Drama")
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  val preferredFilterLite = dramaFilter or goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")) // boolean result
  // filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === true)
  // negation
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))
  //    .show()

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  //  moviesAvgRatingsDF.show()

  // correlation = number between -1 and 1 (Pearson/Spearman)
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  /** corr is an ACTION */

  // Strings
  val carsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //Capilalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))
  //    .show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))
  //    .show()

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(col("Name"), regexp_extract(col("name"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
  //    .show()

  // replace by regex
  vwDF.select(col("Name"), regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace"))
  //    .show()


  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    *
    */
  // version 1:regex
  def getCarNames: List[String] = List("Mercedes-Benz", "Volkswagen", "Ford")

  val regexStringFromList = getCarNames.mkString("|").toLowerCase()
  carsDF.select(col("Name"), regexp_extract(col("Name"), regexStringFromList, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")
  //    .show()
  //version 2: contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()


}
