package part4sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    // define location of Spark's warehouse; instead of 'warehouse' in the root dir
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular dataframe API
  carsDF.select(col("Name")).where(col("Origin") === "USA")
  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select * from cars where Origin = 'USA'
      |""".stripMargin)
  //  americanCarsDF.show()

  // creates a folder in 'spark-warehouse'
  // can run ANY SQL statements
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  //  databasesDF.show()
  //  spark.sql("show tables").show()

  // transfer tables from a DB to Spark tables
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  /**
    * By default, only load the tables... if shouldWrite = true, it will be saved to the warehouse
    *
    * @param tableNames
    * @param shouldWriteToWarehouse
    */
  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach(tableName => {
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  })

    transferTables(List("employees","departments","titles","dept_emp","salaries","dept_manager","movies"))

  // read DF from loaded Spark tables
  //  val employeesDF = spark.read.table("employees")

  //2
  spark.sql(
    """
      |SELECT count(*) from employees WHERE hire_date between '1999-12-31' AND '2001-01-01'
      |""".stripMargin)
    //.show()

  spark.sql(
    """
      |SELECT count(*) from employees WHERE hire_date > '1999-01-01' AND hire_date < '2000-01-01'
      |""".stripMargin)
    //.show()

  // 3
    spark.sql(
      """
        | SELECT de.dept_no, avg(s.salary)
        |  FROM  employees e, dept_emp de, salaries s
        |  WHERE e.hire_date > '1999-01-01' AND e.hire_date < '2000-01-01'
        |  AND e.emp_no = de.emp_no
        |  AND e.emp_no = s.emp_no
        |  group by de.dept_no
        |""".stripMargin
    )
      //.show()

  // 4
  spark.sql(
    """
      | SELECT d.dept_name, avg(s.salary) as avg_sal
      |  FROM  employees e, dept_emp de, salaries s, departments d
      |  WHERE e.hire_date > '1999-01-01' AND e.hire_date < '2000-01-01'
      |  AND e.emp_no = de.emp_no
      |  AND e.emp_no = s.emp_no
      |  AND d.dept_no = de.dept_no
      |  group by d.dept_name
      |  order by avg_sal desc
      | limit 1
      |""".stripMargin
  ).show()

}
