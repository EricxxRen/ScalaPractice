package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TopN {
    public static void main(String[] args) {
        topNsimple();
    }

    /**
     * 取topN1.txt文件中最大的前3个数字
     */
    private static void topNsimple () {
        SparkConf conf = new SparkConf().setAppName("topNsimple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> nums = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/topN1.txt");
        JavaPairRDD<Integer, String> numsRDD = nums.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sorted = numsRDD.sortByKey(false);

        JavaRDD<Integer> result = sorted.map(new Function<Tuple2<Integer, String>, Integer>() {
            public Integer call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1;
            }
        });

        List<Integer> top3 = result.take(3);

        for (Integer ele : top3) {
            System.out.println(ele);
        }

        sc.close();

    }

    /**
     * 分组取前3
     * 取class_topN.txt文件中每个class的前3个数字
     * 参考 https://blog.csdn.net/luofazha2012/article/details/80636858
     * 这里非常重要！！！
     * 还未完成
     */
    private static void groupTopN () {
        SparkConf conf = new SparkConf().setAppName("groupTopN").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/class_topN.txt");

        JavaPairRDD<String, Integer> scores = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words = s.split(":");
                return new Tuple2<String, Integer>(words[0], Integer.valueOf(words[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> grouped = scores.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> result = grouped.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> v2) throws Exception {
                Iterator<Integer> iterator = v2._2.iterator();
                List<Integer> result = new ArrayList<Integer>();
                //TODO Sort algorithm implementation
                return new Tuple2<String, Iterable<Integer>>(v2._1, result);

            }
        });

        sc.close();

    }
}
