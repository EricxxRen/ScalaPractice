package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object SortedWordCounts {
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortedWordCounts").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt")
    val words = lines.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _)
      .map(pair => (pair._2,pair._1)).sortByKey(false).map(pair => (pair._2, pair._1))

    words.foreach(pair => println(pair._1 + ": " + pair._2))

  }
}
