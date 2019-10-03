package com.rxx.Type

/**
 * Scala类型参数
 */
object TypeTests {
  def main(args: Array[String]): Unit = {
    //泛型类测试
    val leo = new Student [Int](111)
//    leo.getStudentId(111)
    val jack = new Student [String]("222")
//    jack.getStudentId("222")

    //泛型函数测试
//    getCard("ABC")
//    getCard [Int] (123)
//    getCard [String] (123) 错误
//    getCard [String] ("ABC")

    //上边界Bounds测试
    val eric = new Teacher("Eric")
    val jesy = new Engineer("Jack")
    val marry = new Person("Marry")
//    val party1 = new Party (eric, jesy)   错误，Engineer类不是Person的子类
//    party1.play
    val party2 = new Party (eric, marry)
//    party2.play

    //下边界Bounds测试
    val father = new Father("Jason")
    val child = new Child("Jack")
    getId(father)
    getId(child)
    getId(jesy)
  }

  //泛型函数
  def getCard [T] (content: T): Unit = {
    if (content.isInstanceOf[Int]) println("card 01: " + content)
    else if (content.isInstanceOf[String]) println("Your card: " + content)
    else println("Error card: " + content)
  }

  //上边界Bounds
  class Person (val name: String) {
    def sayName = println("Hi, I'm " + name)
    def makeFriends (p: Person): Unit = {
      sayName
      p.sayName
    }
  }
  class Teacher (name: String) extends Person(name)
  class Engineer (name: String)
  //[T <: Person]表明泛型T必须是Person类的子类
  class Party [T <: Person] (p1: T, p2: T) {
    def play = p1.makeFriends(p2)
  }

  //下边界Bounds
  class Father (val name: String)
  class Child (name: String) extends Father (name)
  //[R >: Child]表明泛型R必须是Child类的父类
  def getId [R >: Child] (person: R): Unit = {
    if (person.isInstanceOf[Child]) println("Call your Father")
    else if (person.isInstanceOf[Father]) println("Sign here")
    else println("Not Allowed")
  }

}
