package com.rxx.transformRDD;

import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

public class SharedVariable {
    public static void main(String[] args) {
//        broadcastPractise();
        accumulatorPractise();
    }

    private static void broadcastPractise () {
        SparkConf conf = new SparkConf().setAppName("broadcast").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Integer factor = 3;
        //制作广播变量，调用JavaSparkContext的broadcast方法
        //获取的返回结果是Broadcast<T>类型
        //会给每个节点发送一份变量，而不需要给每个task拷贝一份
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> nums = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        JavaRDD<Integer> result = numsRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer integer) throws Exception {
                //获取广播变量值，调用value()方法获取其中封装的值
                int factor = factorBroadcast.value();
                return integer * factor;
            }
        });

        result.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

    private static void accumulatorPractise () {
        SparkConf conf = new SparkConf().setAppName("broadcast").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //在spark2.0中，accumulator使用有变化，需要调用sc.sc().longAccumulator方法
        //不需要指定初始值，默认从0开始，参数为accumulator名
        //参考 https://www.jianshu.com/p/9d6111fc6303
        final LongAccumulator sum = sc.sc().longAccumulator("Sum");

        List<Integer> nums = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        numsRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                //可以调用accumulator的add方法累加值
                sum.add(integer.longValue());
            }
        });

        //Driver中可以调用value()方法读取accumulator值
        System.out.println(sum.value());

        sc.close();
    }
}
