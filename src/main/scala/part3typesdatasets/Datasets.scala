package part3typesdatasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array_contains, avg, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Datasets").config("spark.master", "local").getOrCreate()

  val numberDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/data/numbers.csv")

  //  numberDF.show()
  //  numberDF.printSchema()

  // Convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numberDS: Dataset[Int] = numberDF.as[Int]


  //.show()

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double], //  to allow nulls
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long], //  to allow nulls
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String)

  // 2 - read DF from file
  def readDF(filename: String) = spark.read.option("inferSchema", "true").json(s"src/main/resources/data/${filename}")

  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  val carsDF = spark.read.schema(carsSchema).json("src/main/resources/data/cars.json")
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  numberDS.filter(_ < 100)
  //.show()

  // map, flatMap, fold, reduce, for comprehensions
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show()

  // Note:
  // Datasets
  // - Distributed collection of JVM objects
  // - Most useful when
  //   - we want to maintain type information
  //   - we want clean concise code
  //   - our filters/transformations are hard to express in DF or SQL
  // - Avoid when
  //   - performance is critical: Spark can't optimize transformations

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars have have (HP > 140)
    * 3. Average HP for the entire dataset
    *
    */

  println(s"Number of cars ${carsDS.count()}")
  println(s"Number of powerful cars ${carsDS.filter("Horsepower > 140").count()}")
  val avgHP = carsDS.agg(avg(col("Horsepower"))).first()(0)
  println(s"Avg HP of cars ${avgHP}")

  // from course
  // 2 (use getOrElse in case of a None)
  println(carsDS.filter(_.Horsepower.getOrElse(0l) > 140).count())
  // 3
  print(carsDS.map(_.Horsepower.getOrElse(0l)).reduce(_ + _) / carsDS.count())
  // or
  carsDS.select(avg(col("Horsepower")))

  //.show()


  // Joins
  case class Guitar(id: Long, model: String, make: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  // join => DataFrame; joinWith => Dataset
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(bandsDS, guitarPlayerDS.col("band") === bandsDS.col("id")) //default 'inner'
  guitarPlayerBandsDS.show()

  // exercise
  guitarsDS.joinWith(guitarPlayerDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")
//    .show()
  guitarPlayerDS.joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")
//    .show()

  // grouping
  // groupBy => RelationalGroupedDataset; groupByKey => KeyValueGroupedDataset
  val carsGroupByOriginDS = carsDS.groupByKey(_.Origin)
    .count().as("Count")
    .show()

  // joins and groups are WIDE transformation (can change the number of partitions that back those Datasets); will involve SHUFFLE (is expensive)


}
