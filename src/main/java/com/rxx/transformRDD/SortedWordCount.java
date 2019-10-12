package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SortedWordCount {

    public static void main (String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortedWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt");

        JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(",");
                return Arrays.asList(words).iterator();
            }
        });

        JavaPairRDD<String, Integer> wordPair = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = wordPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //需要进行key，value的反转映射，(apple, 4) -> (4, apple)
        // sort是按照key进行排序的
        JavaPairRDD<Integer, String> countsReversed = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
            }
        });

        //调用sortByKey根据key进行排序
        //false表示进行降序排序
        JavaPairRDD<Integer, String> sorted = countsReversed.sortByKey(false);

        //再次进行反转，得到结果 (4, apple) -> (apple, 4)
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ": " + tuple2._2);
            }
        });

        sc.close();
    }





}
