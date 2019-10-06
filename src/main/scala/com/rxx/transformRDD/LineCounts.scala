package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object LineCounts {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Line Count").setMaster("local")
    val sc = new SparkContext(conf)

    val line = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/linecount.txt")

    //scala与java版本不同，scala没有mapToPair方法，使用map进行映射
    val lines = line.map(line => (line, 1))

    val counts = lines.reduceByKey(_ + _)

    counts.foreach(count => println(count._1 + ": " + count._2))

  }

}
