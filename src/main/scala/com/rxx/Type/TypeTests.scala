package com.rxx.Type

import com.rxx.Type.Student

/**
 * Scala类型参数
 */
object TypeTests {
  def main(args: Array[String]): Unit = {
    val leo = new Student [Int](111)
    leo.getStudentId(111)
    val jack = new Student [String]("222")
    jack.getStudentId("222")

    //泛型函数测试
    getCard("ABC")
    getCard [Int] (123)
//    getCard [String] (123) wrong
    getCard [String] ("ABC")


  }

  //泛型函数
  def getCard [T] (content: T): Unit = {
    if (content.isInstanceOf[Int]) println("card 01: " + content)
    else if (content.isInstanceOf[String]) println("Your card: " + content)
    else println("Error card: " + content)
  }

}
