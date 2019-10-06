package com.rxx.createRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Parallelize");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        //通过并行化集合方式创建RDD
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        Integer result = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(result);

        sc.close();

    }
}
