object alternative {

def main(args : Array[String]) {
  var lt = List(2,3, 8, 1, 6, 10, 20, 33, 7, 4, 22)

  var tp = lt.map(x => lt.map(y => (y, x, y - x))).flatten

  tp.filter(x => x._3 == 2).map(x => (x._1, x._2)).sorted.foreach(println)
}
}
