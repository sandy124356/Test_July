// when you give a random set of Integers , this code will find the elements with only 2 as diff between them and give those pairs..
//without any integers repeating in a sorted order
import org.apache.spark.sql._
import scala.util.control.Breaks._

import org.apache.spark.sql.functions._

object random_numbers_gopal {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(2, 8, 1, 6, 10, 20, 33, 7, 4, 22, 3), 2)

    //val data=spark.sparkContext.parallelize(Seq(1,3,2,4,7,8,9), 2)

    val actual = data.collect().sortBy(x => x)
    //val new_actual_data= actual.map(x=>x+2)

    actual.foreach(println)
    //new_actual_data.foreach(println)

    println("my condition*************************")

    val latest = actual.map(x => (x, x + 2))

    latest.foreach(x => println(x))

    println("final output*************************")
    latest.filter(x => x._1.equals(x._2)).foreach(x => println(x))


    var dict = scala.collection.mutable.Map[Int, Int]()

    for (i <- actual)
    {
      breakable
      {

        if (actual.contains(i + 2))
        {
          if(dict.values.exists(_==i)|| dict.keys.exists(_==i)  )
          {
            break
          }
            else
            dict(i) = i + 2
        }
      }

    }

   dict.toSeq.sortBy(_._1).foreach(println)



  }

}

