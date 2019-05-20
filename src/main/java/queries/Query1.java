package queries;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.WeatherMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import utils.*;

import utils.Parser.ParserCSVHeader;
import utils.Parser.ParserCsvCity;


import java.util.ArrayList;
import java.util.Iterator;



public class Query1 {

    private static String pathToHDFS= "hdfs://172.19.0.5:54310/output";

    private static String pathToFileCondition = "data/prj1_dataset/weather_description.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";



    public static void query1() {

        SparkSession sparkSession= SparkSession.builder()
                .master("local[*]")
                .appName("Weather Analyzer")
                .getOrCreate();



        /*SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Weather Analyzer");*/
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.setLogLevel("ERROR");


        //creating cityRDD
        JavaRDD<String> initialcity= sc.textFile(pathToFileCities/*"hdfs://localhost:54310/data/city_attributes.csv.COMPLETED"*/);
        String header=initialcity.first();
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));

        JavaRDD<CityInfo> cityInfo= initialCityCleaned.map(new Function<String, CityInfo>() {
            @Override
            public CityInfo call(String s) throws Exception {
                return ParserCsvCity.parseLine(s,"query1");
            }
        }).cache();


        //creating weather_condition rdd

        JavaRDD<String> initialweather= sc.textFile(pathToFileCondition/*"hdfs://localhost:54310/data/temperature.csv"*/);
        String headerCityList=initialweather.first();
        String[] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<String> initialW_ConditionCleaned = initialweather.filter(x->!x.equals(headerCityList));

        JavaRDD<WeatherMeasurement> w_measurements =initialW_ConditionCleaned.flatMap(new FlatMapFunction<String, WeatherMeasurement>() {
            @Override
            public Iterator<WeatherMeasurement> call(String s) throws Exception {

                String cvsSplitBy = ",";

                ArrayList<WeatherMeasurement> weatherMeasurements = new ArrayList<>();
                String[] measurements = s.split(cvsSplitBy,-1);

                Lists.newArrayList(cityList).forEach(city-> weatherMeasurements.add(new WeatherMeasurement(city,measurements[0],
                        measurements[ Lists.newArrayList(cityList).indexOf(city)+1])));

                return weatherMeasurements.iterator();
            }
        });


        //filter the null elements and the elements which are not interesting for this query, or malformed
        JavaRDD<WeatherMeasurement> w_meas_notNull=w_measurements.filter(x->!x.getWeather_condition().equals("")
                && !x.getDate().equals("")
                && (x.getMonth().equals("3")||x.getMonth().equals("4")||x.getMonth().equals("5")));


        JavaPairRDD<String,WeatherMeasurement> measuresRDD = w_meas_notNull.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String,CityInfo> cityRDD = cityInfo.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        JavaPairRDD<String,Tuple2<WeatherMeasurement,CityInfo>> joinRDD = measuresRDD.join(cityRDD);


        JavaRDD<WeatherMeasurement> measuresConverted = joinRDD.map(new Function<Tuple2<String, Tuple2<WeatherMeasurement, CityInfo>>, WeatherMeasurement>() {
            @Override
            public WeatherMeasurement call(Tuple2<String, Tuple2<WeatherMeasurement, CityInfo>> t) throws Exception {
                WeatherMeasurement wm = new WeatherMeasurement();
                wm.setCity(t._2()._1().getCity());
                wm.setDate(ConvertDatetime.convert(t._2()._2().getTimezone(),t._2()._1().getDate()));
                wm.setWeather_condition(t._2()._1().getWeather_condition());
                return wm;
            }
        });



        //Getting all info in the tuple and implementing a word count based on (day,year,month,city) keys, which basically counts
        //the number of hours per day characterized by the key specified weather_condition
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYear= measuresConverted.mapToPair(x->new Tuple2<>(new Tuple5<String,String,String,String,String>(x.getDay(),x.getMonth(),x.getYear(),x.getCity(),x.getWeather_condition()),1));
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcount= citiesPerYear.reduceByKey((x,y)->x+y);

        //Filtering couples where weather condition is "sky is clear" and the number of hours per day is greater than 16
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcountClear= citiesPerYearcount.filter(x->x._1()._5().equals("sky is clear") && x._2()>=18);

        //Implementing a second word count  based on (year,month,city) keys, which counts the number of "sky is clear" days
        //for the specified month, year and city contained in the key
        JavaPairRDD<Tuple3<String,String,String>,Integer> daysClear= citiesPerYearcountClear.mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1()._2(),x._1()._3(),x._1()._4()),1));
        JavaPairRDD<Tuple3<String,String,String>,Integer> daysClearCounts= daysClear.reduceByKey((x,y)-> x+y);

        //associating for a given couple (year,city) the list of couples (month,days clear counts)
        JavaPairRDD<Tuple2<String,String>,Tuple2<String, Integer>> monthsClearDaysForYearAndCity= daysClearCounts.mapToPair(x->new Tuple2<>(new Tuple2<>(x._1()._2(),x._1()._3()),new Tuple2<>(x._1()._1(),x._2())));

        //Filtering the previous couples which do not have at least 15 clear days for the month specified in the value
        //and grouping them by (year,city) key
        JavaPairRDD<Tuple2<String,String>,Tuple2<String, Integer>> clearCityPerYearWithMonths=monthsClearDaysForYearAndCity.filter(x->x._2()._2()>=15);
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>> clearMonthsForCityAndForYear= clearCityPerYearWithMonths.groupByKey();


        //filtering the new couples which do not contains three elements(one for each month specified by the query) in the value field
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>> clearCityWithYear=clearMonthsForCityAndForYear.filter(x-> Iterables.size(x._2())==3);

        //mapping the previous cuples to new key-value pairs where the yars represents the key and the value is the city
        //and grouping them by key, sorting by year
        JavaPairRDD<String,String> clearCitiesPerYear= clearCityWithYear.mapToPair(x->new Tuple2<>(x._1()._1(),x._1()._2()));
        JavaPairRDD<String,Iterable<String>> finalResult=clearCitiesPerYear.groupByKey().sortByKey();
        JavaRDD<Tuple3<String,String,String>> RDDForSaving = finalResult.flatMap(new FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple3<String, String, String>>() {
            @Override
            public Iterator<Tuple3<String, String, String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
                ArrayList<Tuple3<String,String,String>> arrFinal= new ArrayList<>();
                t._2().forEach(x->arrFinal.add(new Tuple3<>(/*t._1()+ "_"+*/Integer.toString(Lists.newArrayList(t._2().iterator()).indexOf(x)+1),t._1(),x)));

                return arrFinal.iterator();
            }
        });

        SaveOutput s=new SaveOutput();
        s.saveOutputQuery1(RDDForSaving,sparkSession,pathToHDFS);



        for (int i=0; i< RDDForSaving.collect().size();i++){
            System.out.println("ANNO: "+ RDDForSaving.collect().get(i));
        }

        sc.stop();

    }



}

