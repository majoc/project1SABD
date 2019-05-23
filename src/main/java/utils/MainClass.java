package utils;

import entities.CityInfo;
import entities.TemperatureMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import queries.Query1;
import queries.Query2;
import queries.Query3;
import scala.Tuple2;
import utils.Parser.BuilderCleanerCityRDD;
import utils.Parser.BuilderCleanerTemperatureRDD;

public class MainClass {

    private static String pathToHDFS= "hdfs://172.18.0.5:54310/output";

    private static String pathToFileTemperature = "hdfs://172.18.0.5:54310/dataset/temperature.csv";
    private static String pathToFileHumidity = "hdfs://172.18.0.5:54310/dataset/humidity.csv";
    private static String pathToFileCities = "hdfs://172.18.0.5:54310/dataset/city_attributes.csv";
    private static String pathToFilePressure = "hdfs://172.18.0.5:54310/dataset/pressure.csv";
    private static String pathToFileCondition = "hdfs://172.18.0.5:54310/dataset/weather_description.csv";



   /* private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileHumidity = "data/prj1_dataset/humidity.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";
    private static String pathToFilePressure = "data/prj1_dataset/pressure.csv";
    private static String pathToFileCondition = "data/prj1_dataset/weather_description.csv";*/


    public static void main(String[] args) {


        SparkSession sparkSession= SparkSession.builder()
                //.master("local[*]")
                .appName("Weather Analyzer")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.setLogLevel("ERROR");


        Tuple2<JavaRDD<CityInfo>,Long> cityRDD=BuilderCleanerCityRDD.construct_cleanRDD(sc,pathToFileCities);
        cityRDD._1().cache();

        Query1.query1(sc,cityRDD._1(), cityRDD._2(),sparkSession,pathToHDFS,pathToFileCondition,pathToFileCities);

        Tuple2<JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>>,Long>  temperature =BuilderCleanerTemperatureRDD.construct_CleanRDD(sc,pathToFileTemperature, cityRDD._1());

        temperature._1().cache();

        Query2.query2(sc, sparkSession,cityRDD._1(),temperature._1(), temperature._2(), pathToHDFS ,pathToFileHumidity,pathToFilePressure);

        Query3.query3(sc, sparkSession,temperature._1(), pathToHDFS);

        sc.stop();

    }
}
