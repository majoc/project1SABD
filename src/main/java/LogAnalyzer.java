import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.ApacheAccessLog;
import utils.ValueComparator;

import java.util.Comparator;
import java.util.List;

/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 */
public class LogAnalyzer {

    private static String pathToFile = "data/access.log";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> logLines = sc.textFile(pathToFile);

        /* Convert the text log lines to ApacheAccessLog objects            *
         *  NOTE: we cache them, because multiple transformations           *
         *        and actions will be called on those data.                 */
        JavaRDD<ApacheAccessLog> accessLogs = logLines
                .map(line -> ApacheAccessLog.parseFromLogLine(line))
                .cache();

        /* Calculate statistics based on the content size.                  */
        contentSizeStats(accessLogs);

        System.out.println();

        /* Compute Response Code to Count (take only the first 20)          */
        responseCodeCount(accessLogs);
        System.out.println();

        /* Any IPAddress that has accessed the server more than 100 times   */
        frequentClient(accessLogs, 100);
        System.out.println();

        /* Top-K Requested resources                                        */
        topKRequestedPDFs(accessLogs, 10);

        sc.stop();
    }

    private static void contentSizeStats(JavaRDD<ApacheAccessLog> accessLogs){

        /* Calculate statistics based on the content size.                  *
         *  NOTE: we cache those data                                       */
        JavaRDD<Long> contentSizes = accessLogs
                .map(log -> log.getContentSize())
                .cache();

        /* Compute some statistics, using the cached RDD contentSizes */
        Long totalContentSize   = contentSizes.reduce((a, b) -> a + b);
        long numContentRequests = contentSizes.count();
        Long minContentSize     = contentSizes.min(Comparator.naturalOrder());
        Long maxContentSize     = contentSizes.max(Comparator.naturalOrder());

        System.out.println("Content Size (byte): " +
                " average = " + totalContentSize / numContentRequests +
                ", minimum = " + minContentSize +
                ", maximum = " + maxContentSize);

    }

    private static void responseCodeCount(JavaRDD<ApacheAccessLog> accessLogs){

        JavaPairRDD<Integer, Long>  responseCodePairs   =
                accessLogs.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1L));
        JavaPairRDD<Integer, Long>  responseCodeCounts  =
                responseCodePairs.reduceByKey((a, b) -> a + b);
        List<Tuple2<Integer, Long>> responseCodeToCount =
                responseCodeCounts.take(20); // take the top-20 values

        System.out.println(String.format("Response code counts: %s", responseCodeToCount));

    }

    private static void frequentClient(JavaRDD<ApacheAccessLog> accessLogs, int times){

        List<String> ipAddresses =
                accessLogs.mapToPair(
                        log -> new Tuple2<>(log.getIpAddress(), 1L))
                        .reduceByKey((a, b) -> a + b)
                        .filter(tuple -> tuple._2() > times)
                        .map(tuple -> tuple._1())
                        .collect();

        System.out.println(String.format("IPAddresses > " + times + " times: %s", ipAddresses));

    }

    private static void topKRequestedPDFs(JavaRDD<ApacheAccessLog> accessLogs, int k){

        List<Tuple2<String, Long>> topEndpoints =
                accessLogs.map(log -> log.getEndpoint())
                          .filter(endpoint -> endpoint.toLowerCase().endsWith("pdf"))
                          .mapToPair(endPoint -> new Tuple2<>(endPoint, 1L))
                          .reduceByKey((a, b) -> a + b)
                           // sort data and take the top k endpoints
                          .top(k, new ValueComparator<>(Comparator.<Long>naturalOrder()));

        System.out.println(String.format("Top-%d requested PDF resources: %s", k, topEndpoints));

    }

}
