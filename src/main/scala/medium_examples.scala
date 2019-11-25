import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql._

object medium_examples {

def main (args: Array[String])
  {

    val spark= SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    val someDF= Seq((-1, "one"),(2, "two"),(3,"three")).toDF("num","notation")


    someDF.printSchema()

    someDF.show(false)

      //createDataFrame

    val schemas= StructType(Seq(StructField("number",IntegerType,false),StructField("notation", StringType,false)))

    //val rddData= spark.sparkContext.parallelize(Seq(Row(1,"one"),Row(2, "two"),Row(3,"three")))

    val data=Seq((1,"one"),(2,"two"))

    val rdd=spark.sparkContext.parallelize(data)

    val final_df= spark.createDataFrame(rdd)


    final_df.show()







  }


}
