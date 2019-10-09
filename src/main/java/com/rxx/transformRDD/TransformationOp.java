package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 测试transformation算子
 */
public class TransformationOp {
    public static void main(String[] args) {
//        mapPractise();
//        filterPractise();
//        flatMapPractise();
        groupByKeyPractise();
    }

    /**
     * Array中的数字全部乘2 - Map
     */
    private static void mapPractise () {
        SparkConf conf = new SparkConf().setAppName("Multiply").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        //并行化集合，创建初始RDD
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        //map算子对任何类型的RDD都适用
        //java中，map接收的是Function对象
        //创建的Function对象一定会让你设定第二个泛型参数，这个就是返回类型
        //同时，call()方法的返回类型必须和第二个泛型类型同步
        //在call方法内部对RDD内部每个元素进行计算，所有新的元素组成新的RDD
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

        sc.close();
    }

    /**
     * 过滤Array中的奇数 - Filter
     */
    private static void filterPractise () {
        SparkConf conf = new SparkConf().setAppName("filter").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7);

        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        //filter算子传入的也是Function对象，返回的值是Boolean
        //如果想保留该元素返回true，不想保留返回false
        JavaRDD<Integer> result = numsRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        result.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }

    /**
     * 行拆分为单词 - FlatMap
     */
    private static void flatMapPractise () {
        SparkConf conf = new SparkConf().setAppName("flatMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/wordcount.txt");

        //flatmap算子在java中接收FlatMapFunction对象
        //需要自己定义FlatMapFunction的第二个泛型类型，即返回的元素类型
        //call方法返回的类型不是U，而是Iterator<U>,这里的U也要与第二个泛型类型相同
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> words = Arrays.asList(s.split(","));
                return words.iterator();
            }
        });

        word.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

    /**
     * 每班成绩分组 - groupByKey
     */
    private static void groupByKeyPractise () {
        SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //-----------------使用读取文件方式创建JavaPairRDD----------------------
        JavaRDD<String> scoreRDD = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/marks.txt");

        JavaPairRDD<String, Integer> pairScore = scoreRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] pair = s.split(",");
                String cla = pair[0];
                //注意string转换为integer的方式是valueOf，而不是getInteger
                Integer mark = Integer.valueOf(pair[1]);
                return new Tuple2<String, Integer>(cla, mark);
            }
        });

        //-----------------使用并行化集合方式创建JavaPairRDD----------------------
        List<Tuple2<String, Integer>> scores2 = Arrays.asList(
                new Tuple2<String, Integer>("Class1", 80),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 89)
        );

        //注意此处使用的是parallelizePairs，创建的是JavaPairRDD
        JavaPairRDD<String, Integer> pairScore2 = sc.parallelizePairs(scores2);
        //---------------------------------------------------------------------

        //groupByKey算子，返回的还是JavaPairRDD
        //但此时，第二个参数变成了Iterable<Integer>，是根据key进行分组，每个key都有多个value，形成了Iterable

        JavaPairRDD<String, Iterable<Integer>> result = pairScore.groupByKey();

        result.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> t1) throws Exception {
                System.out.println(t1._1 + ": ");
                Iterator<Integer> marks = t1._2.iterator();
                while (marks.hasNext()) {
                    System.out.println(marks.next());
                }
                System.out.println("============");
            }
        });

        sc.close();

    }
}
