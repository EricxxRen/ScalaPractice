object FuncTests {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4)
    val a2 = a.map(_ * 2)
    for (ele <- a2) println(ele)
  }
}
