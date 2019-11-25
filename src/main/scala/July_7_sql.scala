import org.apache.spark.{ SparkConf, SparkContext, sql}

object July_7_sql {
  def main(args: Array[String])
  {
    println("Start")

    val scon=new SparkConf().setAppName("July_3").setMaster("local[3]")
    val sc=new SparkContext(scon)

    val rdd=sc.textFile("C:\\Users\\u6062310\\Desktop\\sample_file.csv")

    val cnt=rdd.count()

    rdd.foreach(println)



    println(cnt)

    rdd.coalesce(1)



    println("End")

    sc.stop()

  }
}
