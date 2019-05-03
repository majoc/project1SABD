import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.City;
import utils.ParserCsv;

import java.util.ArrayList;
import java.util.List;

public class Query1 {

    private static String pathToFile = "data/prj1_dataset/weat_example.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ArrayList<City> cities =ParserCsv.parseCSV(pathToFile);
        JavaRDD<City> cities_instances=sc.parallelize(cities);
        JavaRDD<City> cities_instances2= cities_instances.filter(x->x!=null && x.getMonth()==3);
        List<City> cities_good_instances=cities_instances2.collect();

        for (int i=0; i<cities_good_instances.size(); i++){
            System.out.println(cities_good_instances.get(i).getCity()+"  "+ cities_good_instances.get(i).getDate()+"  "+ cities_good_instances.get(i).getWeather_condition());
        }


        JavaRDD<String> logLines = sc.textFile(pathToFile);

    }

}
