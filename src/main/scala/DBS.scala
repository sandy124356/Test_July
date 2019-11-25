import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext




object DBS {

  def main(args: Array[String]): Unit = {

  print("main")

  val spark= SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    case class Product (product_id:Int, product_name:String , product_type:String, product_version:String, product_price:String)

    val product_staging_df=spark.read.option("delimiter", "|").option("header", "false").csv("/DBS_INPUT/Product.txt").select("cast(_c0 as Integer) as product_id","_c1 as product_name","_c2 as product_type","_c3 as product_version","_c4 as product_price")

    //val product_staging_case= product_staging_df.as[Product]

    //product_staging_case.createOrReplaceTempView("products_prod")

    //spark.sql("select product_id, product_name, product_type, product_version, replace(product_price,'$','') as product_price from products_prod").createOrReplaceTempView("products_final")


    //spark.sql("select * from products_final").show()


  }

}
