package part2dataframes

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, max}
import org.apache.spark.sql.types._

object Joins extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

//  guitarsDF.printSchema()
//  guitaristDF.printSchema()
//  bandsDF.printSchema()

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
//    .show()

  /**
    * Exercises
    * - show all employees and their max salary
    * - show all employees who were never manager
    * - find the job titles of the best paid 10 employees in the company
    * -
    */
    /// Schemas

    val salariesSchema = new StructType(Array(
      StructField("emp_no", IntegerType),
      StructField("salary", IntegerType),
      StructField("from_date", DateType),
      StructField("to_date", DateType)
    ))

  val employeeSchema = new StructType(Array(
    StructField("emp_no", IntegerType),
    StructField("birth_date", DateType),
    StructField("first_name", StringType),
    StructField("last_name", StringType),
    StructField("gender", StringType),
    StructField("hire_date", DateType)
  ))

  val departmentManagerSchema = new StructType(Array(
    StructField("dept_no", StringType),
    StructField("emp_no",IntegerType),
    StructField("from_date", DateType),
    StructField("to_date", DateType)
  ))

  val departmentEmployeeSchema = new StructType(Array(
    StructField("emp_no",IntegerType),
    StructField("dept_no", StringType),
    StructField("from_date", DateType),
    StructField("to_date", DateType)
  ))

  val titlesSchema = new StructType(Array(
    StructField("emp_no",IntegerType),
    StructField("title", StringType),
    StructField("from_date", DateType),
    StructField("to_date", DateType)
  ))

    /// code

  val defaultOptions = Map(
    "failFast" -> "true",
    "driver"-> "org.postgresql.Driver",
    "url"->"jdbc:postgresql://localhost:5432/rtjvm",
    "user"-> "docker",
    "password"-> "docker",
    "inferSchema"-> "true",
    "dateFormat"-> "yyyy-MM-dd"
  )
  val _salariesDF = spark.read.schema(salariesSchema).format("jdbc").options(defaultOptions).option("dbtable", "public.salaries").load()
  val _employeesDF = spark.read.schema(employeeSchema).format("jdbc").options(defaultOptions).option("dbtable", "public.employees").load()
  val _deptEmployeeDF = spark.read.schema(departmentEmployeeSchema).format("jdbc").options(defaultOptions).option("dbtable", "public.dept_emp").load()
  val _deptManagerDF = spark.read.schema(departmentManagerSchema).format("jdbc").options(defaultOptions).option("dbtable", "public.dept_manager").load()
  val _titlesDF = spark.read.schema(titlesSchema).format("jdbc").options(defaultOptions).option("dbtable", "public.titles").load()

  // Exercise 1: show all employees and their max salary

  _salariesDF.groupBy("emp_no").max("salary")
    // forgot
    .join(_employeesDF, "emp_no")
    .show()

  // Exercise 2: show all employees who were never manager
  val _deptPlusEmployee = _employeesDF.join(_deptEmployeeDF, "emp_no")
  val _deptPlusEmployeePlusManager = _deptPlusEmployee.join(_deptManagerDF, _deptPlusEmployee.col("dept_no") === _deptManagerDF.col("dept_no"), "left_anti")//,"full")
  _deptPlusEmployeePlusManager.select("emp_no")
    .show()
//    .show(deptPlusEmployeePlusManager.count().intValue())

  // Exercise 3: find the job titles of the best paid 10 employees in the company
  val _empSalariesDF = _employeesDF.join(_salariesDF, "emp_no")
  val _highestEarnersDF = _empSalariesDF.groupBy("emp_no").max("salary")
//    .orderBy("max(salary)") // wrong; it orders lowest first
    .orderBy(col("max(salary)").desc)
    .limit(10)
  _highestEarnersDF.join(_titlesDF, "emp_no")
//    .select("title")
    .show()
  _highestEarnersDF.show()

  // Teacher's implementation
  def readTable(tableName:String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName").load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  println("Course Implementation")
  // 1: show all employees and their max salary
//  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").max("salary")
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  // 2: show all employees who were never manager
  val employeeNeverManagerDF = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "left_anti")
  employeeNeverManagerDF.show()

  // 3: find the job titles of the best paid 10 employees in the company
//  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no").max("to_date") (max doesn't work because to_date is text)
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date")) // agg(max) is implemented differently and can work with text
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()

}


