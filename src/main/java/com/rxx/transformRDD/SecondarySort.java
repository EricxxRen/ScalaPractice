package com.rxx.transformRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 * 先根据第一列排序，相同的话根据第二列排序
 * 接收secondarysort.txt
 * 步骤：
 * 1. 实现自定义排序类SecondarySortKey，实现Ordered接口和Serializable接口，在key中实现对多列的排序算法
 * 2. 创建将自定义类(SecondarySortKey)为key，文本"7,4"为value的Tuple2
 * 3. 使用sortByKey算子按照自定义的key进行排序
 * 4. 再次映射，排除自定义的排序类
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/renxiaoxing/Documents/gitRepo/ScalaPractice/Inputs/secondarysort.txt");
        final JavaPairRDD<SecondarySortKey, String> pairRDD = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                //创建将自定义类为key，文本为value的Tuple2
                String[] element = s.split(",");
                SecondarySortKey comp = new SecondarySortKey(Integer.valueOf(element[0]),Integer.valueOf(element[1]));
                return new Tuple2<SecondarySortKey, String>(comp, s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sorted = pairRDD.sortByKey();

        JavaRDD<String> result = sorted.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        result.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}


