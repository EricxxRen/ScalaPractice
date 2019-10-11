package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object SharedVariables {

  def main (args: Array[String]): Unit = {
//    broadcastTest()
    accumulatorTest()
  }

  def broadcastTest (): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast = sc.broadcast(factor)

    val nums = Array(1,2,3,4,5)
    val numsRDD = sc.parallelize(nums)

    numsRDD.map(num => num * factorBroadcast.value).foreach(num => println(num))
  }

  def accumulatorTest(): Unit = {
    val conf = new SparkConf().setAppName("broadcast").setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.longAccumulator("sum")

    val nums = Array(1,2,3,4,5)
    val numsRDD = sc.parallelize(nums)

    numsRDD.foreach(num => sum.add(num.toLong))

    println(sum.value)

  }

}
