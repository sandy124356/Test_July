//http://abc.org/checkout?website_discount_code=8675309&language=English
//http://abc.org/checkout?website_discount_code=8675309&language=English
//http://cdw.org/checkout?spiceworks_discount_code=8675309&language=English
//http://cdw.org/checkout?spiceworks_discount_code=8675309&language=English

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object urls {

  def main (args: Array[String]): Unit =
  {
    val spark= SparkSession.builder().master("local[*]").getOrCreate()


    import spark.implicits._
    import sql.functions._

    val dff=spark.read.format("csv").option("delimiter", "?").load("C:\\Users\\u6062310\\Desktop\\urls.txt")

    dff.show()

    dff.withColumnRenamed("_c0","websitename").withColumnRenamed( "_c1","params").show()



























    val df=spark.read.format("csv").load("C:\\Users\\u6062310\\Desktop\\urls.txt")



    case class schema(website: String, param1: String )






    df.createOrReplaceTempView("urls")

    //val tabledf=spark.sql("select replace(replace(_c0,'&',','), '?', ',') from urls")


    val tdf=df.withColumn("finegrained",
      regexp_replace(
      regexp_replace(
      regexp_replace(col("_c0"),"\\?",","),
        "\\&",","),
        "\\=",",")
    )

    val replaced_df=df.withColumn("finegrained", regexp_replace(col("_c0"),"\\?|\\&|\\=", ","))


    //tdf2.write.csv("C:\\Users\\u6062310\\Desktop\\new_urls.txt")
      //tdf.show()
    //val ttdf=tdf.withColumn("urls", split($"finegrained", ",")).select($"urls".getItem(0).as("col1"),$"urls".getItem(1).as("col2")
    //,$"urls".getItem(2).as("col3"))

    val each_col_df=replaced_df.withColumn("urls", split($"finegrained", ",")).select($"urls".getItem(0).as("col1"),$"urls".getItem(1).as("col2")
    ,$"urls".getItem(2).as("col3"))

    //tdf.withColumn("urls",split(col("finegrained"),",")   ).select(col("urls").getItem(0).as("col11"),col("urls").getItem(1).as("col22")).show()



    //ttdf.show()

    //ttdf.write.csv("C:\\Users\\u6062310\\Desktop\\urls_replaced.txt")

    each_col_df.show()

    spark.close()
  }

}
