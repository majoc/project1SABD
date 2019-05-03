package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.util.List;
/*

public class PreprocessingQuery1 {


    private static String pathToFile = "data/prj1_dataset/weat_example.csv";

    public static int preprocessDataset(JavaSparkContext sc) {


        JavaRDD<String> weatherDescriptionFile = sc.textFile(pathToFile);
        int x=0;
        if(x==0){
        JavaRDD<String> CityNames =
                weatherDescriptionFile.map(
                        // line -> OutletParser.parseJson(line))         // JSON
                        line -> OutletParser.parseCSV(line))            // CSV
                        .filter(x -> x != null && x.getProperty().equals("1"));
*/
/*
        for(int i=0; i<items.length; i++) {
            System.out.println(items[i]);
        }
*//*





        return 1;



    }

    public static void main(String[] args){

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        preprocessDataset(sc);
        sc.stop();
    }


}
*/
