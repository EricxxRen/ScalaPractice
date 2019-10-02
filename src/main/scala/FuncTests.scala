object FuncTests {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4)

    val a2 = a.map(_ * 2)
//    for (ele <- a2) println(ele)

    val a3 = a.filter(_ % 2 == 0).map(math.pow(_, 2).toInt)
//    for (ele <- a3) println(ele)

    val a4 = (1 to 100).filter((x: Int) => (x % 2 != 0)).map((x: Int) => x * x).reduceLeft(_ + _)
//    println(a4)

    //模式匹配
    def gradeJudge (grade: String): String = {
      grade match {
        case "A" => "Excellent"
        case "B" => "good"
        case "C" => "so so"
        case "D" => "work harder"
        case _ => "fail"
      }
    }
//    println(gradeJudge("v"))

    //模式匹配+if守卫
    def gradeJudge2 (name: String, grade: String) = {
      grade match {
        case "A" => println(name + ": Excellent")
        case "B" => println(name + ": Good")
        case "C" if name == "Leo" => println(name + ", you got C Come On")
        case "C" => println(name + ": So so")
        case _ if name == "Leo" => println(name + ", Come On")
        case _ => println(name + ": Fail")
      }
    }
//    gradeJudge2("Eric", "A")
//    gradeJudge2("Leo", "F")

    //模式匹配+变量赋值
    def gradeJudge3 (name: String, grade: String) = {
      grade match {
        case "A" => println(name + ": Excellent")
        case "B" => println(name + ": Good")
        case "C" => println(name + ": So so")
        case _grade => println(name + ": " + _grade + ", you need to work harder")
      }
    }
//    gradeJudge3("Eric", "A")
//    gradeJudge3("Leo", "E")

    //类型模式匹配
    def animalTest (animal: Animal) = {
      animal match {
        case dog: Dog => println("A Dog")
        case cat: Cat => println("A Cat")
        case _: Animal => println("WTF")
      }
    }
    val an1: Animal = new Dog
    val an2: Animal = new Cat
    val an3 :Animal = new Racoon
//    animalTest(an1)
//    animalTest(an2)
//    animalTest(an3)

    //Array类型匹配
    def greeting (arr: Array[String]): Unit = {
      arr match {
        case Array("Leo") => println("Hi, Leo!")
        case Array(a1, a2, a3) => println("Hello, " + a1 + ", " + a2 + " and " + a3)
        case Array("Eric", _*) => println("Hi, Eric, Why do't you introduce your friends?")
        case _ => println("Who are you?")
      }
    }
//    greeting(Array("Leo"))
//    greeting(Array("Marry", "Jen", "Emma"))
//    greeting(Array("Eric", "Jack", "Tom", "Welsley"))
//    greeting(Array("Jason"))

    //List类型匹配
    def greeting2 (list: List[String]): Unit = {
      list match {
        case "Leo"::Nil => println("Hi, Leo!")
        case a1::a2::a3::Nil => println("Hello, " + a1 + ", " + a2 + " and " + a3)
        case "Eric"::tail => println("Hi, Eric, Why do't you introduce your friends?")
        case _ => println("Who are you?")
      }
    }
//    greeting2(List("Leo"))
//    greeting2(List("Marry", "Jen", "Emma"))
//    greeting2(List("Eric", "Jack", "Tom", "Welsley"))
//    greeting2(List("Jason"))

    //case class匹配
    def identifier (person: Person): Unit = {
      person match {
        case Teacher(name, subject) => println("Teacher: " + name + ", your subject is " + subject)
        case Student(name, classroom) => println("Student: " + name + ", your classroom is " + classroom)
        case _ => println("You are not authorized")
      }
    }
    val p1: Person = Teacher("Jack", "Electrical")
    val p2: Person = Student("Eric", "AstroPhysics")
    val p3: Person = Worker("Leo")

//    identifier(p1)
//    identifier(p2)
//    identifier(p3)

    val grades = Map("Eric" -> "A", "Tom" -> "B", "Bo" -> "B")

    def getGrade (name: String): Unit = {
      val grade = grades.get(name)
      grade match {
        case Some(grade) => println(name + ", your grade is " + grade)
        case None => println("Sorry, " + name + " your grade is not ready yet")
      }
    }

    getGrade("Eric")
    getGrade("Jack")

  }

  class Animal {}
  class Dog extends Animal {}
  class Cat extends Animal {}
  class Racoon extends Animal {}

  class Person
  case class Teacher (name: String, subject: String) extends Person
  case class Student (name: String, classroom: String) extends Person
  case class Worker (name: String) extends Person
}
