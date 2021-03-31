package part2dataframes

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  guitarsDF.printSchema()
  guitaristDF.printSchema()
  bandsDF.printSchema()

  // inner joins
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristDF.join(bandsDF, joinCondition, "inner") // inner is default value
//    .show()

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls where the data is missing
  guitaristDF.join(bandsDF, joinCondition, "left_outer")
//    .show()

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls where the data is missing
  guitaristDF.join(bandsDF, joinCondition, "right_outer")
//    .show()

  // full outer = everything in the inner join + all the rows in BOTH tables, with nulls where the data is missing
  guitaristDF.join(bandsDF, joinCondition, "outer")
//    .show()

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_semi")
//    .show()

  // anti-joins (inverse of semi-join) = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristDF.join(bandsDF, joinCondition, "left_anti")
//    .show()

  // things to keep in mind
  //guitaristsBandsDF.select("id", "band").show()// this crashes, "Reference 'id' is ambiguous, could be: id, id.;"

  //option 1 - rename the column on which we are joining (now both the guitaristDF and bandsDF have a column "band" where the join will operate on
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
    //.show()

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))//.show()

  // option 3 - rename the offending column and keep the data (still have duplicate data)
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId"))
//    .show()

  // using complex types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))
    .show()

}


