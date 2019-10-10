package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
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
//        groupByKeyPractise();
//        reduceByKeyPractise();
//        sortByKeyPractise();
//        joinPractise();
        cogroupPractise();
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

    /**
     * 班级总分 - reduceByKey
     */
    private static void reduceByKeyPractise () {
        SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/marks.txt");

        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] pair = s.split(",");
                String cla = pair[0];
                Integer score = Integer.valueOf(pair[1]);
                return new Tuple2<String, Integer>(cla, score);
            }
        });

        //reduceByKey算子接收Function2对象，Function2对象有3个泛型参数
        //第1，2个泛型参数代表原始rdd中参数的类型，也与call方法的参数类型相同，
        //第3个泛型类型代表每次reduce操作返回值得类型
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //对每个key都会将其value传入call方法
            //从而聚合出每个key对应的value
            //然后将key和对应的value组合成为Tuple2，作为新RDD的元素
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t1) throws Exception {
                System.out.println(t1._1 + ": " + t1._2);
            }
        });

        sc.close();

    }

    /**
     * 分数排序 - sortByKey
     */
    private static void sortByKeyPractise () {
        SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> scores = Arrays.asList(
                new Tuple2<Integer, String>(65, "jack"),
                new Tuple2<Integer, String>(90, "Etic"),
                new Tuple2<Integer, String>(23, "tom"),
                new Tuple2<Integer, String>(79, "jason")
        );

        JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(scores);

        //sortByKey算子为根据key进行排序
        //sortByKey()为升序，sortByKey(false)为降序
        JavaPairRDD<Integer, String> soredRDD = pairRDD.sortByKey();
        JavaPairRDD<Integer, String> soredRDD2 = pairRDD.sortByKey(false);

        soredRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> t1) throws Exception {
                System.out.println(t1._1 + ": " + t1._2);
            }
        });

        sc.close();

    }

    /**
     * 打印分数 - join
     */
    private static void joinPractise () {
        SparkConf conf = new SparkConf().setAppName("join").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "jack"),
                new Tuple2<Integer, String>(2, "Etic"),
                new Tuple2<Integer, String>(3, "tom"),
                new Tuple2<Integer, String>(4, "jason"),
                //在下面的scores中，没有key为5的tuple2，不会再join中显示
                new Tuple2<Integer, String>(5, "john")
        );

        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 645),
                new Tuple2<Integer, Integer>(2, 234),
                new Tuple2<Integer, Integer>(3, 678),
                new Tuple2<Integer, Integer>(4, 1213),
                //在上面的names中，没有key为6的tuple2，不会再join中显示
                new Tuple2<Integer, Integer>(6, 2341)
        );

        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);

        //使用join算子关联2个rdd
        //会根据key进行join返回pairRDD
        //JavaPairRDD的第一个泛型为之前2个JavaPairRDD的key的类型，是根据key进行的join
        //第二个泛型为Tuple2<v1,v2>的类型，v1和v2分别为原始rdd的value的类型
        //注意：只有在key匹配的情况下才会返回，names有而scores没有或names没有而scores有的不会返回
            //如((1，2),(1，3),(1，4))和((1，5),(2，6),(2，7))进行join会得到：
            //((1，(2,5)),(1，(3,5)),(1，(4,5)))
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nameRDD.join(scoreRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t1) throws Exception {
                System.out.println(t1._1 + ") " + t1._2._1 + ": " + t1._2._2);
            }
        });

        sc.close();
    }

    /**
     * 打印分数 - cogroup
     */
    private static void cogroupPractise () {
        SparkConf conf = new SparkConf().setAppName("cogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //一个key下有多个value
        List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "jack"),
                new Tuple2<Integer, String>(1, "john"),
                new Tuple2<Integer, String>(2, "Etic"),
                new Tuple2<Integer, String>(3, "tom"),
                new Tuple2<Integer, String>(3, "tomas"),
                new Tuple2<Integer, String>(4, "jason")
        );

        //一个key下有多个value
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 645),
                new Tuple2<Integer, Integer>(1, 5435),
                new Tuple2<Integer, Integer>(2, 234),
                new Tuple2<Integer, Integer>(3, 678),
                new Tuple2<Integer, Integer>(3, 567),
                new Tuple2<Integer, Integer>(3, 678),
                new Tuple2<Integer, Integer>(4, 1213),
                new Tuple2<Integer, Integer>(4, 761)

        );

        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);

        //使用cogroup算子关联2个rdd
        //会根据key进行cogroup返回pairRDD
        //JavaPairRDD的第一个泛型为之前2个JavaPairRDD的key的类型，是根据key进行的cogroup
        //第二个泛型为Tuple2<v1,v2>的类型，v1和v2分别为原始rdd的value的类型的Iterable
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = nameRDD.cogroup(scoreRDD);

        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t1) throws Exception {
                System.out.println(t1._1 + ": " + t1._2._1 + "|" + t1._2._2);
            }
        });

        sc.close();
    }

}
