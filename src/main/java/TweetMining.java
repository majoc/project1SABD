
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.Tweet;
import utils.TweetParser;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 *  The Java Spark API documentation: http://spark.apache.org/docs/latest/api/java/index.html
 *
 *  Now we use a dataset with 8198 tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the tweets:
 *  - Find all people mentioned in tweets
 *  - Count how many times each person is mentioned
 *  - Find the top-10 mentioned people
 */
public class TweetMining {

    private static String pathToFile = "data/reduced-tweets.json";
    private static Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Tweet mining");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaRDD<String> rawTweets = sc.textFile(pathToFile);

        // Extract and parse tweet
        JavaRDD<Tweet> tweets =
                rawTweets.map(line -> TweetParser.parseJson(line));

        // Extract words within a tweet
        JavaRDD<String> words =
                tweets.flatMap(tweet -> Arrays.asList(SPACE.split(tweet.getText())).iterator());

        // Identify a mention (i.e., @user)
        JavaRDD<String> mentions =
                words.filter(word -> word.startsWith("@") && word.length() > 2).cache();


        // Query #1: count mentions
        System.out.println("Query 1 - Count Mentions: mentions.count() = " + mentions.distinct().count());


        // Query #2: compute the top-10 rank
        List<Tuple2<Integer, String>> mostMentioned =
                mentions.mapToPair(mention -> new Tuple2<>(mention, 1))
                        .reduceByKey((x, y) -> x + y)
                        .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                        .sortByKey(false)
                        .take(10);

        System.out.println("Query 2 - Top 10 of the most mentioned users");
        for (Tuple2<Integer, String> mm : mostMentioned){
            System.out.println(mm._2() + ": " + mm._1());
        }

        sc.stop();

    }
}