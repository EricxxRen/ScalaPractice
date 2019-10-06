package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

/**
 * Array中的数字全部乘2
 */
public class MultiplyByTwo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Multiply").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        //
        JavaRDD<Integer> result = numsRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer o) throws Exception {
                return o * 2;
            }
        });

        result.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
