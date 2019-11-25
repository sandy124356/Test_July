import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object maps_exps {

def main(args:Array[String]): Unit =
  {

    val spark= SparkSession.builder().master("local[*]").getOrCreate()

    val df= spark.read.csv("C:\\Users\\u6062310\\Desktop\\car_plates.txt")

    print(df.getClass())


    val vrdd=spark.sparkContext.textFile("C:\\\\Users\\\\u6062310\\\\Desktop\\\\car_plates.txt")

print(vrdd.getClass())

print("here----->")
    vrdd.foreach(println)
    print("here-----22222222222222222222222222222222222>")
    //val new_rdd=vrdd.map(x=>x.split(",")(4))

    vrdd.map(x=>x.split(",")(4)).foreach(println)
    println("at least completed till here----->")



    //new_rdd.foreach(println)

    df.printSchema()

    //df.show(20)
    //df.filter(df("_c1")==="Bill Meyer").show()

    //df.filter("_c1='Bill Meyer'").show()


    //df.where("_c1!='Bill Meyer'").show()

    //df.where(trim(col("_c1"))="Bill Meyer")


    //df.withColumn("trimmed",trim(col("_c1"))).filter("trimmed='Bill Meyer'").show()



    //df.filter(df("trimmed")==="Bill Meyer").show()

    //df.filter("trimmed='Bill Meyer'").show()


  }

}


