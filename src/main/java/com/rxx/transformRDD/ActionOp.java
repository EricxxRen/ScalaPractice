package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings(value = {"unused", "unchecked"})
public class ActionOp {
    public static void main(String[] args) {
//        reducePractise();
//        collectPractise();
//        countPractise();
//        takePractise();
//        saveAsTextFilePractise();
        countByKeyPractise();
    }

    private static JavaSparkContext getSc (String name) {
        SparkConf conf = new SparkConf().setAppName(name).setMaster("local");
        return new JavaSparkContext(conf);
    }

    private static void reducePractise () {
        JavaSparkContext sc = getSc("reduce");

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        Integer result = numsRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(result);

        sc.close();
    }

    private static void collectPractise () {
        JavaSparkContext sc = getSc("collect");

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        JavaRDD<Integer> result = numsRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        //此处不使用foreach操作在远程集群上遍历数组
        //使用collect将分布在集群上的RDD结果拉取到本地
        //一般不建议使用，在结果RDD数据量比较大的情况下：
        //性能差，产生大量网络传输，同时容易产生OOM(内存溢出)异常
        //因此，一般推荐使用foreach操作对结果RDD进行处理
        List<Integer> collect = result.collect();

        for (Integer num : collect) {
            System.out.println(num);
        }

        Iterator<Integer> iterator = collect.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        sc.close();
    }

    private static void countPractise () {
        JavaSparkContext sc = getSc("count");

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        System.out.println(numsRDD.count());

        sc.close();

    }

    private static void takePractise () {
        JavaSparkContext sc = getSc("count");

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        //take操作与collect类似，也是从远程集群上获取RDD的数据
        //但是collect是获取所有数据，take是获取前n个数据
        List<Integer> top3Nums = numsRDD.take(3);

        for (Integer num : top3Nums) {
            System.out.println(num);
        }

        sc.close();
    }

    private static void saveAsTextFilePractise () {

        JavaSparkContext sc = getSc("saveAsTextFile");

        //此处为保存到hdfs集群上的设置，注意不需要setMater
//        SparkConf conf = new SparkConf().setAppName("saveAsTextFile");
//        JavaSparkContext sc =  new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numsRDD = sc.parallelize(nums);

        //注意，这里的path只能写文件夹路径，实际的数据被保存到saveAsTestFile文件夹下的part-00000文件中
        numsRDD.saveAsTextFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Output/saveAsTestFile");
        //此处为保存到hdfs集群上
//        numsRDD.saveAsTextFile("hdfs://spark1:9000/sparktest/saveAsTestFilePractise");
        sc.close();
    }

    private static void countByKeyPractise () {
        JavaSparkContext sc = getSc("countByKey");

        List<Tuple2<String, String>> infos = Arrays.asList(
                new Tuple2<String, String>("class1", "eric"),
                new Tuple2<String, String>("class1", "eric"),
                new Tuple2<String, String>("class2", "eric"),
                new Tuple2<String, String>("class1", "eric"),
                new Tuple2<String, String>("class2", "eric"),
                new Tuple2<String, String>("class2", "eric")
        );

        JavaPairRDD<String, String> pairs = sc.parallelizePairs(infos);

        //统计每个key下元素个数
        Map<String, Long> result = pairs.countByKey();

        for (Map.Entry<String, Long> entry : result.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
