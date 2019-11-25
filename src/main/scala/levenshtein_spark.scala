
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object levenshtein_spark {


  def main(args : Array[String]): Unit =
  {

    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    import spark.implicits._

    var df1=Seq(("san","HYD"),("mera","DEL"),("amir","MUMB")).toDF("id","name")



    df1.printSchema()

    //df.show()

    var df2=Seq(("sany","black"),("money","blue"),("mira","green")).toDF("id","color")

    //df1.withColumn("sound", soundex(col("name"))).show()


   // df2.withColumn("sound",soundex(col("name"))).show()


    df1.join(df2,levenshtein(df1("id"),df2("id"))<4).show()


    var df3=Seq(("amir","rani"),("money","honey"),("mira","amir")).toDF("id","color")

    df3.withColumn("lavenshtein", levenshtein(col("id"),col("color"))).show()


  }

}
