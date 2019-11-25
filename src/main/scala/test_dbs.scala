import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test_dbs {

  def main(args: Array[String]): Unit =
  {

    val spark= new sql.SparkSession.Builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val product_schema= StructType(List(
      StructField("product_id",IntegerType,false),
      StructField("product_name",StringType,false),
      StructField("product_type",StringType,true),
      StructField("product_version",StringType,true),
      StructField("product_price",StringType,true),
      StructField("_corrupt_record",StringType,true)
                                      )
                                    )
    val customer_schema= StructType(List(
      StructField("customer_id",IntegerType, false),
      StructField("customer_first_name",StringType, false),
      StructField("customer_last_name",StringType, true),
      StructField("phone_number",StringType, true),
      StructField("_corrupt_record",StringType,true)

                                        )
                                    )

    val sales_schema= StructType(List(
      StructField("transaction_id",IntegerType, false),
      StructField("customer_id",StringType, false),
      StructField("product_id",StringType, false),
      StructField("timestamp",IntegerType, true),
      StructField("total_amount",IntegerType, true),
      StructField("total_quantity ",IntegerType, true),
      StructField("_corrupt_record",StringType,true)

    )
    )

    val Refund_schema= StructType(List(
      StructField("refund_id",IntegerType, false),
      StructField("original_transaction_id",IntegerType, false),
      StructField("customer_id",IntegerType, false),
      StructField("product_id",IntegerType, false),
      StructField("timestamp",StringType, false),
      StructField("refund_amount",IntegerType, true),
      StructField("refund_quantity",IntegerType, true),
      StructField("_corrupt_record",StringType,true)

    )
    )



    val products_Staging_df=spark.read.option("header", false).option("delimiter", "|").schema(product_schema).csv("C:\\Users\\u6062310\\Desktop\\DBS\\Product.txt")

    val customer_staging_df=spark.read.option("header",false).option("delimiter", "|").schema(customer_schema).csv("C:\\Users\\u6062310\\Desktop\\DBS\\Customer.txt")


    //products_Staging_df.printSchema()
    //products_Staging_df.show()
    //customer_staging_df.show()

    val newdf=products_Staging_df.withColumn("sounds",soundex(col("product_name")))

    newdf.show()

    //println({soundex(col("laplop"))})

   //val abc= soundex(col("laplop"))

    val data=List("LAPTOP", "gym", "jym", "mail","male").toDF("name")




    val df2=data.withColumn("sounds2",soundex(col("name")))

    df2.show()

    print("here is output")

    newdf.join(df2,newdf("sounds")===df2("sounds2"), "full_outer").select("name","sounds").show()


    //val data1=List(("laplop","laplap"), ("gym","jym"),( "male", "mail")).toDF("name1","name2" )



    //data1.show()

    //data1.withColumn("lavenstein",levenshtein(col("name1"),col("name2"))).show()




    //products_Staging_df.filter("product_id==1112 ").show()


    //products_Staging_df.filter($"product_id".isNull).count()
    //println(s" values i am looking for are these many ${products_Staging_df.filter("product_version like '%2'").filter("product_type=='P105' or product_type=='P106'").show()}")

    //  val num=products_Staging_df.count()

    //println("number of like 2 are " ,num )

    //println (s"number of like 2 are ${products_Staging_df.filter("product_id==1112").count()}")

    //products_Staging_df.createOrReplaceTempView("products_staging")

    //print(products_Staging_df.count())

    //spark.sql("select product_id,product_name,product_type,product_version, replace(product_price,'$','') as product_price, _corrupt_record  from products_staging").createOrReplaceTempView("Products_final")

    //spark.sql("select * from products_final").show(100)


    //products_Staging_df.printSchema()





  }


}
