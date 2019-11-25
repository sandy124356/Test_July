object fun {




def main(args: Array[String]): Unit = {

  format("1$0");
}

  def format(S: String) = {
    try {
      println("in here")
      var formatted:Double=0

      formatted=S.replaceAll("[$,{,]", "").trim.toDouble
      println(formatted)

    } catch {
      case e: Exception => println("Exception while converting currency values from input file")
    }

  }
}