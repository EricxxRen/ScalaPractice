package com.rxx.createRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class FileInput {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("File Input").setMaster("local");
        //HDFS File Input modify
        //SparkConf conf = new SparkConf().setAppName("Local File Input")
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt");
        //HDFS File Input modify
        //JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/wordcount.txt")

        JavaRDD<Integer> counts = lines.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        Integer result = counts.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(result);
    }
}
