import org.apache.spark.sql.SparkSession


object micron {

  def main(args: Array[String]): Unit =
  {
    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._
    val evenrdd=spark.sparkContext.parallelize((1 to 100).toList).filter(x=>(x%2==0))

    val oddrdd=spark.sparkContext.parallelize((1 to 100).toList).filter(x=>(x%2==1))

    //evenrdd.collect().foreach(println)

    //oddrdd.collect().foreach(println)

    evenrdd.zipWithIndex().collect().foreach(println)

    oddrdd.zipWithIndex().collect().foreach(println)


    val evendf=evenrdd.zipWithIndex().toDF("even","id1")

    val odddf=oddrdd.zipWithIndex().toDF("odd","id2")

    evendf.show()

    odddf.show()

   val newdf= evendf.join(odddf,evendf("id1")===odddf("id2")).withColumn("even+odd", evendf("even")+odddf("odd"))

   newdf.select("id1","even","odd","even+odd").orderBy("id1").show(50)

  }

}
