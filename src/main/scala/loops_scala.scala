


object loops_scala {
def main(args: Array[String]): Unit =
  {
    for (i <- 1 to 10)
    {
      println(scala.util.Random.nextInt())
    }

    val lists =List("www","sss","rrr")

    for (i <- lists)
    {

      println(i)
    }

    lists.foreach(println)
  }



}
