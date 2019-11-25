import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dbs_7_sep {

  def main(args: Array[String]): Unit =
  {

    val spark= new sql.SparkSession.Builder().master("local[*]").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val book_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "book").option("user", "snemmal").option("password", "password123").load()
    book_df.show()

    book_df.createOrReplaceTempView("book")


    val review_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "review").option("user", "snemmal").option("password", "password123").load()
    review_df.show()

    review_df.createOrReplaceTempView("review")


    val user_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "user").option("user", "snemmal").option("password", "password123").load()

    user_df.createOrReplaceTempView("user")


    val genre_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "genre").option("user", "snemmal").option("password", "password123").load()
    genre_df.show()

    genre_df.createOrReplaceTempView("genre")

    val user_genre_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "user_genre").option("user", "snemmal").option("password", "password123").load()
    user_genre_df.show()

    user_genre_df.createOrReplaceTempView("user_genre")


    val book_genre_df = spark.read.format("jdbc").option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "book_genre").option("user", "snemmal").option("password", "password123").load()
    book_genre_df.show()


    book_genre_df.createOrReplaceTempView("book_genre")

   // val sql_input1 = "select round(avg(rating), 2) avg, b.book_title from review r join book b on b.book_id=r.book_id  group by b.book_id order by avg desc"

    //val new_df= spark.sql(sql_input1)

    //new_df.show()

   val sql_input = "select round(avg(rating), 2) avg_rating, b.book_title, g.genre from review r join book b on b.book_id=r.book_id join book_genre bg on bg.book_id=b.book_id join genre g on g.g_id=bg.gid group by b.book_title,g.genre order by avg_rating desc"

    val agg_df= spark.sql(sql_input)

    agg_df.show()


    agg_df.write.format("jdbc").mode(SaveMode.Overwrite).option("url", "jdbc:mysql://aws-mysql.crvnvcpirne1.ap-south-1.rds.amazonaws.com:3306/bookdb").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "agg_data_from_spark").option("user", "snemmal").option("password", "password123").save()

    val games_df= List("games", "pokemon", "game", "fantasy", "hiking"  )
    games_df.foreach(println)
    val tech_df=List("microsoft", "apple", "facebook", "IBM")
    val devotional_df=List("god", "miracle", "pray")
    val love_df=List("love", "kiss", "romance", "girlfriend", "girl")
    val comedy_df=List("comedy", "fun", "laugh", "smile")







  }


}


