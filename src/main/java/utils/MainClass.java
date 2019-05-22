package utils;

import entities.CityInfo;
import entities.TemperatureMeasurement;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import queries.Query1;
import queries.Query2;
import queries.Query3;
import scala.Tuple2;
import utils.Parser.ParserCsvCity;
import utils.Parser.ParserCsvTemperature;

public class MainClass {

    private static String pathToHDFS= "hdfs://172.18.0.5:54310/output";


    private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileHumidity = "data/prj1_dataset/humidity.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";
    private static String pathToFilePressure = "data/prj1_dataset/pressure.csv";
    private static String pathToFileCondition = "data/prj1_dataset/weather_description.csv";


    public static void main(String[] args) {


        SparkSession sparkSession= SparkSession.builder()
                .master("local[*]")
                .appName("Weather Analyzer")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.setLogLevel("ERROR");


        //Query1.query1(sc,sparkSession,pathToHDFS,pathToFileCondition,pathToFileCities);

        Long partialPreprocessing = System.currentTimeMillis();

        JavaRDD<String> initialcity= sc.textFile(pathToFileCities);
        String header=initialcity.first();
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));


        //creating city rdd for query2, filled up with nation info
        JavaRDD<CityInfo> cityRDD= initialCityCleaned.map((Function<String, CityInfo>)
                s -> ParserCsvCity.parseLine(s,"query2")).cache();

        partialPreprocessing=System.currentTimeMillis()-partialPreprocessing;

        Tuple2<JavaRDD<TemperatureMeasurement>, Long>  temperature =ParserCsvTemperature.construct_CleanRDD(sc,pathToFileTemperature);

        temperature._1().cache();



        Query2.query2(sc, sparkSession, cityRDD, temperature._1(), temperature._2() + partialPreprocessing, pathToHDFS ,pathToFileHumidity,pathToFilePressure);

        Query3.query3(sc, sparkSession, cityRDD, temperature._1(), temperature._2(), pathToHDFS);

        sc.stop();

    }
}
