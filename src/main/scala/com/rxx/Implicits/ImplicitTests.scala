package com.rxx.Implicits

/**
 * 隐式转换测试
 */
object ImplicitTests {
  def main(args: Array[String]): Unit = {

    //隐式转换
    val leo = new Student("Leo")
    val jack = new Elder("Jack")
    val tom = new Worker("Tom")
//    buyTicket(leo)
//    buyTicket(jack)
//    buyTicket(tom) 错误，Worker类没有被隐式转换

    //隐式转换强化现有类
    val don = new Man("Don")
//    don.emitLaser

    //隐式参数测试
    //implicit val signPen定义了一个隐式值，会被scala自动注入到隐式参数中
    implicit val signPen = new SignPen
    signForWork("Eric")
  }

  //隐式转换测试
  class SpecialPerson (val name: String)
  class Student (val name: String)
  class Elder (val name: String)
  class Worker (val name: String)
  //当obj为student或Elder时，将obj的类型隐式转换为SpecialPerson
  implicit def object2SpecialPerson (obj: Object) :SpecialPerson = {
    if (obj.getClass == classOf[Student]) {
      val _stu = obj.asInstanceOf[Student]
      new SpecialPerson(_stu.name)
    } else if (obj.getClass == classOf[Elder]) {
      val _old = obj.asInstanceOf[Elder]
      new SpecialPerson(_old.name)
    } else Nil
  }
  var ticketNum = 0
  def buyTicket (p: SpecialPerson) = {
    ticketNum += 1
    println("T-" + ticketNum)
  }

  //隐式转换加强现有类
  class Man (val name: String)
  class Superman (val name: String) {
    def emitLaser = println(name + ": biu~~~~~~~")
  }
  implicit def man2Superman (man: Man): Superman = {
    new Superman(man.name)
  }

  //隐式参数
  class SignPen {
    def write (content: String) = println(content)
  }
  //(implicit signPen: SignPen)定义了一个隐式参数，scala会在1.SignPen的伴生对象中，2.当前作用域中寻找隐式值
  def signForWork (name: String) (implicit signPen: SignPen): Unit = {
    signPen.write(name + " arrived!")
  }
}
