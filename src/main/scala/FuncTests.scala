object FuncTests {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4)
    val a2 = a.map(_ * 2)
//    for (ele <- a2) println(ele)
    val a3 = a.filter(_ % 2 == 0).map(math.pow(_, 2).toInt)
    val a4 = (1 to 100).filter((x: Int) => (x % 2 != 0)).map((x: Int) => x * x).reduceLeft(_ + _)
//    for (ele <- a4) println(ele)
    println(a4)
  }
}
