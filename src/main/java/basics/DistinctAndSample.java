package basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class DistinctAndSample {

    private static boolean SAMPLING_REPLACEMENT = true;
    private static double SAMPLING_PROBABILITY = 0.5;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 1, 2, 6));

        // Transformations
        JavaRDD<Integer> distinctNumbers = input.distinct();
        List<Integer> distinct = distinctNumbers.collect();
        System.out.println("Distinct Numbers: " + distinct);
        System.out.println();

        JavaRDD<Integer> sampleNumbers = input.sample(SAMPLING_REPLACEMENT, SAMPLING_PROBABILITY);
        List<Integer> sampled = sampleNumbers.collect();
        System.out.println("Sampled Numbers: " + sampled);

        sc.stop();
    }

}
