package com.rxx.createRDD

import org.apache.spark.{SparkConf, SparkContext}

object FileInputS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("File Input").setMaster("local")
    //HDFS File Input modify
    //val conf = new SparkConf().setAppName("File Input")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt", 2)
    //HDFS File Input modify
    //val lines = sc.textFile("hdfs://spark1:9000/wordcount.txt", 2)

    val result = lines.map(count => count.length).reduce(_ + _)

    println(result)

  }

}
