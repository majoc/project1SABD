package basics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SimpleJoin {

    private static final String fileTransactions = "data/transactions.txt";
    private static final String fileUsers = "data/users.txt";

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SimpleJoin");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> transactionInputFile = sc.textFile(fileTransactions);
        JavaPairRDD<String, Integer> transactionPairs =
                transactionInputFile.mapToPair(
                s -> {
                    String[] transactionSplit = s.split("\t");
                    return new Tuple2<>(transactionSplit[0], Integer.valueOf(transactionSplit[1]));
                }).cache();

        JavaRDD<String> customerInputFile = sc.textFile(fileUsers);
        JavaPairRDD<String, String> customerPairs =
                customerInputFile.mapToPair(
                s ->  {
                    String[] customerSplit = s.split("\t");
                    return new Tuple2<>(customerSplit[0], customerSplit[1]);
                }).cache();

        List<Tuple2<String, Tuple2<Integer, String>>> result =
                transactionPairs.join(customerPairs)
                                .collect();

        for (Tuple2<String, Tuple2<Integer, String>> joinResult : result){

            System.out.println("Key: " + joinResult._1()
                    + "; Join: <"
                    + joinResult._2()._1() + ", "
                    +joinResult._2()._2() + ">");

        }

        sc.close();
    }

}
