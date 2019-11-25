package com.eurowings
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Filter_columns {


  def main(args: Array[String]): Unit ={

    val spark= new SparkSession.Builder().master("local[*]").getOrCreate()

    import spark.implicits._

    val df=spark.read.format("csv").option("header","true").load("hdfs://eurowings/eurowings/listings_2018-01-01.csv")

    df.printSchema()

  }
}
