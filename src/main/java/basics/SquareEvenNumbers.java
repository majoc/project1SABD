package basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.regex.Pattern;

public class SquareEvenNumbers {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Square Even Number");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // Transformations
        JavaRDD<Integer> evenNumbers = input.filter(x -> (x % 2 == 0));
        JavaRDD<Integer> squaredEvenNumbers = evenNumbers.map(x -> x * x);

        for (Integer i : squaredEvenNumbers.collect()){
            System.out.println(i);
        }

        sc.stop();
    }

}
