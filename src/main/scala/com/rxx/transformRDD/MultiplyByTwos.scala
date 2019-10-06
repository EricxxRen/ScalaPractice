package com.rxx.transformRDD

import java.util
import java.util.{Arrays, List}

import org.apache.spark.{SparkConf, SparkContext}

object MultiplyByTwos {

  def main (args: Array[String]) = {

    val conf = new SparkConf().setAppName("Multiply").setMaster("local")
    val sc = new SparkContext(conf)

    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numsRDD = sc.parallelize(nums);

    val result = numsRDD.map(value => value * 2).foreach(value => println(value))

  }
}
