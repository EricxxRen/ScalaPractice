package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object ActionOps {

  def main(args: Array[String]): Unit = {
//    reduceTest()
//    collectTest()
//    countTest()
//    takeTest()
    countByKeyTest()
  }

  def getSc (name: String): SparkContext = {
    val conf = new SparkConf().setAppName(name).setMaster("local")
    new SparkContext(conf)
  }

  def reduceTest (): Unit = {
    val sc = getSc("reduce")

    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums)

    println(numsRDD.reduce(_ + _))

  }

  def collectTest (): Unit = {
    val sc = getSc("collect")

    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums)

    val array = numsRDD.map(i => i * 2).collect()

    for (ele <- array) println(ele)
  }

  def countTest (): Unit = {
    val sc = getSc("collect")

    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums)

    println(numsRDD.count())
  }

  def takeTest (): Unit = {
    val sc = getSc("collect")

    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums)

    val top3Nums = numsRDD.take(3)

    for (num <- top3Nums) println(num)
  }

  def saveAsTextFileTest (): Unit = {
    val sc = getSc("saveAsTextFile")

    val nums = Array(1,2,3,4,5,6,7,8,9,10)
    val numsRDD = sc.parallelize(nums)

    numsRDD.saveAsTextFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Output/saveAsTestFileS")

  }

  def countByKeyTest (): Unit = {
    val sc = getSc("countByKey")

    val classes = Array(
      Tuple2("class1", "eric"),
      Tuple2("class2", "eric"),
      Tuple2("class1", "eric"),
      Tuple2("class2", "eric"),
      Tuple2("class1", "eric"),
    )

    val pairs = sc.parallelize(classes)

    val classMap = pairs.countByKey()

    for (ele <- classMap) {
      println(ele._1 + ": " + ele._2)
    }
  }

}
