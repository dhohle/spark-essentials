package part5lowlevel

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source

object RDDs extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String) = {
    Source.fromFile(filename)
      .getLines()
      .drop(1) // if you have a header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv").map(_.split(",")).filter(tokens => tokens(0).toUpperCase() == tokens(0)).map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._

  val stocksDS = stocksDF.as[StockValue]
  // is a dataset
  val stocksRDD3 = stocksDS.rdd
  // is a list of Rows
  val stocksRDD4 = stocksDF.rdd

  // RDD -> DF (lose type information)
  val numbersDF = numbersRDD.toDF("number") // one argument per column
  // RDD -> DS (keep type information)
  val numbersDS = spark.createDataset(numbersRDD)

  // second lecture

  /// Transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION
  print(msCount)

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  //  implicit val stockOrdering:Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  //  implicit val stockOrdering = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  implicit val stockOrdering = Ordering.fromLessThan((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // very expensive
  println(groupedStocksRDD.count())

  // Partitioning
  //  val repartitionedStocksRDD = stocksRDD.repartition(30) // is expensive
  //  repartitionedStocksRDD.toDF().write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks30")

  /**
    * Repartitioning is EXPENSIVE. Involves Shuffling.
    * Best practice: partition EARLY, then process that.
    * Size of partition between 10~100 MB
    */


  // coalesce
  //  val coalescedRDD = repartitionedStocksRDD.coalesce(15)  //does NOT involve full shuffling (should be less than the previous partition size)
  //  coalescedRDD.toDF().write
  //    .mode(SaveMode.Overwrite)
  //    .parquet("src/main/resources/data/stocks15")

  // exercise
  case class Movie(title: String, genre: String, rating: Double)

  // 1:
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd
  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(_.genre == "Drama").filter(_.rating > 6)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingsByGenreRDD = moviesRDD.groupBy(_.genre).map({
    case (genre, movies) => GenreAvgRating(genre = genre, rating = movies.map(_.rating).sum / movies.size)
  })


  //  moviesRDD.toDF().show()
  //  genresRDD.toDF().show()
  //  goodDramasRDD.toDF().show()
  avgRatingsByGenreRDD.toDF().show()
  moviesRDD.toDF().groupBy(col("genre")).avg("rating").show()


}
