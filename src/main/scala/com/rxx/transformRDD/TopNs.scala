package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object TopNs {

  def main(args: Array[String]): Unit = {
    topNsimpleTest()
  }

  def topNsimpleTest (): Unit = {
    val conf = new SparkConf().setAppName("topNsimpleTest").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/topN1.txt")
    val numsRDD = lines.map(num => (Integer.valueOf(num), num)).sortByKey(false)
      .map(pair => pair._1)

    val top3 = numsRDD.take(3)

    for (ele <- top3) {
      println(ele)
    }

  }

}
