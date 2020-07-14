package sparkMysql

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.sql._
import org.apache.sql.types._
import org.apache.sql.functions._
import org.apache.sql.streaming._
import java.sql.{Connection,DriverManager,ResultSet}

object Mysql{

	def main(args: Array[String]){

		Logger.getLogger("org").setLevel(Level.ERROR)

		val spark = SparkSession
					.builder()
					.appName("SparkMysqlConnection")
					.master("local[*]")
					.config("spark.sql.warehoushe.dir","C:/temp/")
					.getOrCreate()

		val mysql_df = spark.readStream.format("jdbc")
					   .option("url","jdbc:mysql://127.0.0.1:3306/test")
					   .option("driver","com.mysql.jdbc.Driver")
					   .option("dbtable","test")
					   .option("user","root")
					   .option("password","password")
					   .load()
		//mysql_df.printSchema()
		//mysql_df.show()
		//mysql_df.select("testcase").show
		//mysql_df.groupBy("testcase").count().show()

		//creating view of table
		//mysql_df.createGlobalTempView("people")
		//val data_stream = spark.sql("SELECT * FROM global_temp.people where fileId=2").show();

		/*
		//read a csv file from local

		val df = spark.readStream
				 .format("csv")
				 .option("header","true") //first line in file has headers
				 .option("mode","DROPMALFORMED")
				 .load("Filepath")

		df.show()
		*/

		val schema = StructType(
						Array(StructField("id",StringType),
							StructField("fileId",StringType),
							StructField("testcase",StringType),
							StructField("action",StringType)
							StructField("date_des",StringType),
							StructField("status",StringType)
						)
					)

		val df = spark.readStream.format("csv")
					.option("header","true")
					.option("sep",",")
					.schema(schema)
					.csv("D:/course/spar/*")

		val WriteToSQLQuery = mysql_df.writeStream.foreach(new ForeachWriter[Row]{
			var connection:java.sql.Connection = _
			var statement:java.sql.Statement = _

			val jdbcUsername = "root"
			val jdbcPassword = "password"
			val jdbcHostname = "localhost"
			val jdbcport = 3306
			val jdbcDatabase = "test"
			val driver = "com.mysql.jdbc.Driver"
			val jdbc_url = "jdbc:mysql://127.0.0.1:3306/test"

			def open(partitionId: Long, version: Long):Boolean = {
				Class.forName(driver)
				connection = DriverManager.getConnection(jdbc_url, jdbcUsername,jdbcPassword)
				statement = connection.createStatement
				true
			}

			def process(value: Row):Unit ={
				val id = value(0)
				val fileId = value(1)
				val testcase = value(2)
				val action = value(3)
				val date_des = value(4)
				val status = value(5)

				val valueStr = "'" + id + "," + "'" + fileId + "," + "'" + testcase + "," + "'" + action + "," + "'" + date_des + "," + "'" + status + "'"
				statement.execute("INSERT INTO "+ "test.temptest"+" VALUES("+valueStr+")")
			}

			def close(errorOrNull: Throwable): Unit = {
				connection.close
			}
		})

		var streamingQuery = WriteToSQLQuery.start()
		//spark.streams.awaitAnyTermination()
	}
}