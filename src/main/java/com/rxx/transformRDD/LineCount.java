package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Line Count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/linecount.txt");
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String o) throws Exception {
                return new Tuple2<String, Integer>(o, 1);
            }
        });

        JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        countRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + ": " + tuple2._2);
            }
        });

        sc.close();
    }
}
