package com.rxx.transformRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationOPs {

  def main (args: Array[String]) = {
//    mapTest()
//    filterTest()
//    flatMapTest()
    groupByKeyTest()
  }

  /**
   * Array中的数字全部乘2
   */
  def mapTest (): Unit = {
    val conf = new SparkConf().setAppName("Multiply").setMaster("local")
    val sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numsRDD = sc.parallelize(nums);

    numsRDD.map(value => value * 2).foreach(value => println(value))

  }

  /**
   * 过滤Array中的奇数
   */
  def filterTest (): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5, 6, 7)

    val numsRDD = sc.parallelize(nums)

    numsRDD.filter(num => num%2 == 0).foreach(num => println(num))
  }

  /**
   * 行拆分为单词
   */
  def flatMapTest (): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt")

    lines.flatMap(line => line.split(",")).foreach(word => println(word))
  }

  /**
   * 每班成绩分组 - groupByKey
   */
  def groupByKeyTest (): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    //-----------------使用读取文件方式创建JavaPairRDD----------------------
    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/marks.txt")
    val scorePair = lines.map(line => {
      //此处注意：Java中有mapToPair方法，scala中没有，直接采用map方法
      //下面是采用一行数据形成一个Tuple2数据
      (line.split(",")(0), Integer.valueOf(line.split(",")(1)))
    }).groupByKey()

    //-----------------使用并行化集合方式创建JavaPairRDD----------------------
    val lines2 = Array(
      Tuple2("class1", 80),
      Tuple2("class2", 89),
      Tuple2("class2", 83))
    val scorePair2 = sc.parallelize(lines2).groupByKey()
    //--------------------------------------------------------------------

    scorePair2.foreach(pair => {
      println(pair._1 + ": ")
      pair._2.foreach(sc => println(sc))
      println("---------------")
    })
  }

}
