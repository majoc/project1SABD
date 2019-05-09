package queries;


import entities.CityInfo;
import entities.TemperatureMeasurement;
import entities.WeatherMeasurement;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple3;
import utils.ConvertDatetime;
import utils.Parser.ParserCsvCity;
import utils.Parser.ParserCsvTemperature;
import utils.Parser.ParserCsvW_Condition;

import java.util.ArrayList;

public class Query3 {

    private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";
    private static String year1 = "2016";
    private static String year2 = "2017";


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Weather Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        top3cityMaxDifference(sc);


        sc.stop();

    }


    private static void top3cityMaxDifference(JavaSparkContext sc) {

        ArrayList<TemperatureMeasurement> temperature = ParserCsvTemperature.parseCSV(pathToFileTemperature);
        ArrayList<CityInfo> citiesArray = ParserCsvCity.parseCSV(pathToFileCities,"query3");

        JavaRDD<TemperatureMeasurement> temperatureRDD =sc.parallelize(temperature);

        JavaRDD<TemperatureMeasurement> temperatureRDDclean = temperatureRDD.filter(x->x!=null);

        JavaRDD<CityInfo> cityRDD = sc.parallelize(citiesArray);

        JavaPairRDD<String,TemperatureMeasurement> cityTemperatures = temperatureRDDclean.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String,CityInfo> cityCityInfo = cityRDD.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> joinRDD = cityTemperatures.join(cityCityInfo);

        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> convertedJoin = joinRDD.mapToPair(x->new Tuple2<>(x._1(),
                new Tuple2<>(new TemperatureMeasurement(x._1(),ConvertDatetime.convert(x._2()._2().getTimezone(),
                        x._2()._1().getDate()),x._2()._1().getTemperature()),x._2()._2()))).cache();

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureFirstGroup = temperatureMean(convertedJoin,"6","7","8","9");

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureSecondGroup = temperatureMean(convertedJoin,"1","2","3","4");

        JavaPairRDD<Tuple3<String,String,String>,Tuple2<Double,Double>> temperatureJoin = temperatureFirstGroup.join(temperatureSecondGroup);

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureDiff = temperatureJoin.mapToPair(x->new Tuple2<>(x._1(),Math.abs(x._2()._1()-x._2()._2()))).cache();

        //2016
        //JavaPairRDD<Tuple3<String,String,String>,Double> temperatureDiffFirstYear = temperatureDiff.filter(x->x._1()._2().equals(year1));

        //2017
        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureDiffSecondYear = temperatureDiff.filter(x->x._1()._2().equals(year2));




    }

    private static JavaPairRDD<Tuple3<String,String,String>,Double> temperatureMean(JavaPairRDD<String, Tuple2<TemperatureMeasurement, CityInfo>> convertedJoin, String m1, String m2, String m3, String m4) {

        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> temperatureFiltered = convertedJoin.filter(x->(x._2()._1().getYear().equals(year1) ||
                x._2()._1().getYear().equals(year2)) && (x._2()._1().getMonth().equals(m1) ||
                x._2()._1().getMonth().equals(m2) || x._2()._1().getMonth().equals(m3) ||
                x._2()._1().getMonth().equals(m4)) && ( Integer.parseInt(x._2()._1().getHour()) <= 15 && Integer.parseInt(x._2()._1().getHour()) >= 12));

        JavaPairRDD<Tuple3<String,String,String>,Tuple2<Double,Integer>> temperaturePartial = temperatureFiltered.mapToPair
                (x->new Tuple2<>(new Tuple3<>(x._2()._2().getNation(),x._2()._1().getYear(),x._1()),new Tuple2<>(Double.parseDouble(x._2()._1().getTemperature()),1)));

        JavaPairRDD<Tuple3<String,String,String>,Tuple2<Double,Integer>> temperatureByKey = temperaturePartial.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> t1, Tuple2<Double, Integer> t2) throws Exception {

                return new Tuple2<>(t1._1()+t2._1(),t1._2()+t2._2());
            }
        });

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureMeanRDD = temperatureByKey.mapToPair(x->new Tuple2<>(x._1(),x._2()._1()/x._2()._2()));

        return temperatureMeanRDD;

    }



}
