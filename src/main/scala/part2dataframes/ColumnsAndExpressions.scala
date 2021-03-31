package part2dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ColumnsAndExpressions extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //  carsDF.show()
  //  carsDF.printSchema()
  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  // col === column; df.col (is a bit different, but don't know how yet)

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Acceleration"),
    'Year, // Scala Symbol, auto-converted to column (spark.implicits._ needed)
    $"Horsepower", // fancier interpolated string, returns a Column object (implicits._ needed)
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    //    weightInKgExpression //has name: "(Weight_in_lbs / 2.2)"
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )


  // DF processing
  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  var carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column name
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCars = carsDF.filter(col("Origin") =!= "USA")
  val europeanCars2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression string
  val americanCardsDF = carsDF.filter("Origin = 'USA'")
  val americanCardsDF2 = carsDF.filter(col("Origin") === "USA")
  // chain filters
  val americanPowerfulCardsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCardsDF2 = carsDF.filter(
    (col("Origin") === "USA").and(col("Horsepower") > 150)
  )
  val americanPowerfulCardsDF3 = carsDF.filter(
    col("Origin") === "USA" and col("Horsepower") > 150
  )
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150 ")

  //  unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  //  allCountriesDF.show()


  /**
    * Exercises
    * - read movies
    * - select 2 columns
    * - create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * - Select all COMEDY movies (Major_Genre = Comedy) with IMDB > 6
    */

  val moviesDF = spark.read.option("nullValue", "").option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.printSchema()

  // exer 1
  val moviesDF2Cols = moviesDF.select("Release_Date", "Title")
  // alt selects
  moviesDF.select(col("Release_Date"), moviesDF.col("Title"))
  moviesDF.select($"Release_Date", expr("Title"))
  moviesDF.selectExpr("Title", "Release_Date")
  moviesDF2Cols.show()


  // exer 2
  //  val summedIncome = //moviesDF.withColumn("Summed_income", expr("US_DVD_Sales + US_Gross + Worldwide_gross"))
  moviesDF.select(col("Title"), col("US_Gross"), col("Worldwide_Gross"), col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total Gross"))
  moviesDF.select("Title", "US_Gross", "Worldwide_Gross").withColumn("Summed_income", col("US_Gross") + col("Worldwide_gross"))
    .show()
  moviesDF.selectExpr(
    "Title", "US_Gross", "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross")
  //    .show()
  //  summedIncome.show()

  // exer 3
  //  val comedyMoviesDF = moviesDF.filter("IMDB_Rating >= 6 and Major_Genre ='Comedy'")
  moviesDF.select("Title", "IMDB_Rating").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  moviesDF.select("Title", "IMDB_Rating").where(col("Major_Genre") === "Comedy").where(col("IMDB_Rating") > 6)
  moviesDF.select("Title", "IMDB_Rating").where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
      .show()


}
