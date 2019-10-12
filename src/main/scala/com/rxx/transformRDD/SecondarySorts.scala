package com.rxx.transformRDD

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySorts {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySorts").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/secondarysort.txt");
    lines.map(line => {
      val first = Integer.valueOf(line.split(",")(0))
      val second = Integer.valueOf(line.split(",")(1))
      (new SecondarySortKey(first, second), line)
    }).sortByKey().map(pair => pair._2).foreach(pair => println(pair))
  }

}
