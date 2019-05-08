package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import utils.*;

import java.util.ArrayList;

public class Query2 {

    private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";

    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ArrayList<TemperatureMeasurement> temperature = ParserCsvTemperature.parseCSV(pathToFileTemperature);
        ArrayList<CityInfo> citiesArray = ParserCsvCity.parseCSV(pathToFileCities,"query2");

        JavaRDD<TemperatureMeasurement> temperatureRDD =sc.parallelize(temperature);

        JavaRDD<CityInfo> cityRDD = sc.parallelize(citiesArray);

        sc.stop();

    }


}
