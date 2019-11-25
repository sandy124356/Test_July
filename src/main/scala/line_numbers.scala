import org.apache.spark.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession



object line_numbers {

  def main(args: Array[String]) {

  val sconn = new SparkConf().setAppName("line_count").setMaster("local[*]")
  val sc = new SparkContext(sconn)

  val spark= SparkSession.builder().getOrCreate()

    import spark.implicits

    val rdd_file = sc.textFile("C:\\Users\\u6062310\\Desktop\\lines_sample.txt")




    //rdd_file.collect().foreach(println)

    rdd_file.filter(x=>x.toString.contains("santhosh")).foreach(println)

    print("****************************")
    rdd_file.collect().map(x=>(x,1)).foreach(println)


    rdd_file.zipWithIndex().filter(x=>x.toString().contains("santhosh")).foreach(println)

    rdd_file.zipWithIndex().map(x=>(x,1)).foreach(println)


    //rdd_file.zipWithIndex().filter(x=>x.toString().contains"santhosh")

    val df=spark.read.csv("C:\\Users\\u6062310\\Desktop\\lines_sample.txt")

    df.filter("_c0 like '%santhosh%'").show()


  }
}
