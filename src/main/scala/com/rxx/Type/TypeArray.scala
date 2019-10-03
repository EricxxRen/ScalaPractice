package com.rxx.Type

/**
 * 测试泛型数组实例化
 */
object TypeArray {
  def main(args: Array[String]): Unit = {
    val beef = new Meat("Beef")
    val lamb = new Meat("Lamb")
    val pork = new Meat("Pork")

    val foods = packageF(beef, lamb, pork)
    for (ele <- foods) println(ele.name)
  }

  class Meat (val name: String)
  class Vegetable (val name: String)

  def packageF [T: Manifest] (foods: T*) = {
    val foodPackage = new Array[T](foods.length)
    for (i <- 0 until foods.length) {
      foodPackage(i) = foods(i)
    }
    foodPackage
  }
}
