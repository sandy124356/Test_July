package gopal

import org.apache.spark.sql._

object random_nums_sort {

  def main (args: Array[String]): Unit =
  {

    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._
    val data=spark.sparkContext.parallelize(Seq(2,8,1,6,10,20,33,7,4,22,3), 2)

    //val data=spark.sparkContext.parallelize(Seq(1,3,2,4,7,8,9), 2)

    val actual=data.collect().sortBy(x=>x)

    val diff= actual.map(x=>x-2).filter(x=>x>0)

    //actual.foreach(println)
    //diff.foreach(println)

    val df=data.toDF("id")



    df.createOrReplaceTempView("table1")

    val newdf=spark.sql("select tab2.id id1, tab1.id id2, case when tab1.id>tab2.id then tab1.id-tab2.id else tab2.id-tab1.id end col3 from table1 tab1 cross join  table1 tab2 where tab1.id>tab2.id")

    val newdf2=newdf.filter("col3=2").sort("id2")

    newdf2.show()
    newdf2.createOrReplaceTempView("filtered")

    spark.sql("select id1 from (select id1 from  filtered union select id2 from filtered ) order by id1").show()

    newdf.show(131)

    newdf.select("id1", "id2").collectAsList().getClass





  }

}
