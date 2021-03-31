package part2dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataSources extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Data Source and Formats")
    .config("spark.master", "local")
    .getOrCreate()

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

  /**
    * Reading a DF:
    * - format
    * - schema (optional)
    * - path
    * - zero or more options
    */
  val carsDf = spark.read
    .schema(carsSchema) // enforce a schema
    .format("json")
    .option("mode", "failFast") // dropMalformed, permissive (default), failFast (fails if any record is wrongly types)
    .option("path", "src/main/resources/data/cars.json") // alternative to call this in `load()`
    .load()

  // an alternative using a options (map) instead of a list of option's
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json"
    ))
    .load()


  /*
    writing DFs
    - format
    - save mode = overwrite, append, ignore, errorIfExists, (update)
    - path
    - zero or more options
   */
  carsDf.write
    .format("json")
    .mode(SaveMode.Overwrite)
    //    .option("path", "src/main/resources/data/cars_dupe.json")
    //    .save()
    .save("src/main/resources/data/cars_dupe.json")


  // JSON flags
  spark.read
    //    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true") //if the JSON is created by single quotes instead of double quotes
    .option("compression", "uncompressed") // bzip1, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  spark.read
    .schema(stocksSchema)
    .option("sep", ",") // , (default)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true") // ignores the first row
    //    .option("format", "csv")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet (default storage format in Spark)
  carsDf.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet") // save() <- is parquet by default

  // Text Files; every row is a value
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  // note: make sure the Docker Postgres Database is running
  // 'docker-compose up' in this projects Root
  // Note: make sure no other instance of Postgres is running
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()


  /**
    * Exersize: Read movies DF
    */
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()

  moviesDF.write
    .option("sep", "\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/output/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy")// is default compression, so not needed
    .parquet("src/main/resources/data/output/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()




}
