package com.rxx.WordCount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Word Count")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt", 1)

    val words = lines.flatMap(line => line.split(","))

    val pairs = words.map(word => (word, 1))

    val result = pairs.reduceByKey(_ + _)

    result.foreach(pair => println(pair._1 + ": " + pair._2))
  }
}
