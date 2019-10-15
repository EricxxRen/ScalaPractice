package com.rxx.transformRDD

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

/**
 * 参考 https://blog.csdn.net/luofazha2012/article/details/80636858
 * 非常重要！！！
 * 输入文件见/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/TopN_Advanced
 */
object DistributedTopN {
  def main(args: Array[String]): Unit = {
    val UniqueTopN_Path = "/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/TopN_Advanced/UniqueTopN.txt"
    val NonUniqueTopN_Path = "/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/TopN_Advanced/NonUniqueTopN.txt"
    val GroupTopN_Path = "/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/TopN_Advanced/GroupTopN.txt"

    uniqueTopNTest(UniqueTopN_Path, 3)

  }

  def uniqueTopNTest (path: String, topn: Integer): Unit = {
    val conf = new SparkConf().setAppName("uniqueTopN").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(path)
    val topN: Broadcast[Integer] = sc.broadcast(topn)
    val pairs = lines.map(pair => {
      val tag = pair.split(" ")(0)
      val num = pair.split(" ")(1)
      (num.toInt, pair)
    })

    val partitions = pairs.mapPartitions(iterator => {
      var sortedMap = SortedMap.empty[Int, String]
      iterator.foreach({ tuple => {
          sortedMap += tuple
          if (sortedMap.size > topN.value) sortedMap = sortedMap.takeRight(topN.value)
        }
      })
      sortedMap.takeRight(topN.value).toIterator
    })

    val allPartitions = partitions.collect()
    val finalTopN = SortedMap.empty[Int, String].++(allPartitions)
    val result1 = finalTopN.takeRight(topN.value)

    result1.foreach(line => println(line._1 + ": " + line._2))

  }

}
