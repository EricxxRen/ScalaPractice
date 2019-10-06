package com.rxx.createRDD

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeTestS {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Parallelized").setMaster("local")
    val sc = new SparkContext(conf)

    val nums = Array (1,2,3,4,5,6,7,8,9,10);
    val numsRDD = sc.parallelize(nums,2)

    val result = numsRDD.reduce(_ + _)

    println(result)

  }


}
