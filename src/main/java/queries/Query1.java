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


        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.setLogLevel("ERROR");


        //creating cityRDD
        JavaRDD<String> initialcity= sc.textFile(pathToFileCities/*"hdfs://localhost:54310/data/city_attributes.csv.COMPLETED"*/);
        String header=initialcity.first();

        //filtering content except header line
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));

        //creating a city object RDD
        JavaRDD<CityInfo> cityInfo= initialCityCleaned.map((Function<String, CityInfo>)
                s -> ParserCsvCity.parseLine(s,"query1")).cache();


        //creating weather_condition rdd
        JavaRDD<String> initialweather= sc.textFile(pathToFileCondition/*"hdfs://localhost:54310/data/temperature.csv"*/);

        //filetering header and constructin measurement RDD
        String headerCityList=initialweather.first();
        String[] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<WeatherMeasurement> w_measurements= initialweather.filter(x->!x.equals(headerCityList))
                   .flatMap((FlatMapFunction<String, WeatherMeasurement>) s -> {

                       String cvsSplitBy = ",";

                       ArrayList<WeatherMeasurement> weatherMeasurements = new ArrayList<>();
                       String[] measurements = s.split(cvsSplitBy,-1);

                       Lists.newArrayList(cityList).forEach(city-> weatherMeasurements.add(new WeatherMeasurement(city,measurements[0],
                               measurements[ Lists.newArrayList(cityList).indexOf(city)+1])));

                       return weatherMeasurements.iterator();
                   });


        //filter the null elements and the elements which are not interesting for this query, or malformed
        JavaRDD<WeatherMeasurement> w_meas_notNull=w_measurements.filter(x->!x.getWeather_condition().equals("")
                && !x.getDate().equals("")
                && (x.getMonth().equals("3")||x.getMonth().equals("4")||x.getMonth().equals("5")));

        //We need to join city info contained in CityInfo instances with the measumerement datetime
        //in order to convert the UTC value in local hour

        //RDD with city as key and wheather measure object as value
        JavaPairRDD<String,WeatherMeasurement> measuresRDD = w_meas_notNull.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        //RDD with city as key and CityInfo object as value
        JavaPairRDD<String,CityInfo> cityRDD = cityInfo.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        //Applying inner join between previous RDD and saving relevant info in measurement object
        //with converted datetime
        JavaRDD<WeatherMeasurement> measuresConverted = measuresRDD.join(cityRDD)
            .map((Function<Tuple2<String, Tuple2<WeatherMeasurement, CityInfo>>, WeatherMeasurement>) t -> {
                t._2()._1().setCity(t._2()._1().getCity());
                t._2()._1().setDate(ConvertDatetime.convert(t._2()._2().getTimezone(),t._2()._1().getDate()));
                t._2()._1().setWeather_condition(t._2()._1().getWeather_condition());
                return t._2()._1();
            });



        //Getting all info in the tuple and implementing a word count based on (day,month,year,city,condition) keys,
        // which basically counts
        //the number of hours per day characterized by the key specified weather_condition
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcountSkyIsClear= measuresConverted.mapToPair(x->
                new Tuple2<>(new Tuple5<>(x.getDay(),x.getMonth(),x.getYear(),x.getCity(),x.getWeather_condition()),1))
                 .reduceByKey((x,y)->x+y)

        //Filtering couples where weather condition is "sky is clear" and the number of hours per day is greater
        //a fixed treshold equal to 18 hours
                .filter(x->x._1()._5().equals("sky is clear") && x._2()>=18);

        //Implementing a second word count  based on (month,year,city) keys, which counts the number of "sky is clear" days
        //for the specified month, year and city contained in the key
        JavaPairRDD<Tuple3<String,String,String>,Integer> daysClearCounts= citiesPerYearcountSkyIsClear.mapToPair(x->
                new Tuple2<>(new Tuple3<>(x._1()._2(),x._1()._3(),x._1()._4()),1))
                .reduceByKey((x,y)-> x+y);


        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>>  clearMonthsForCityAndForYear= daysClearCounts
                //associating for a given couple (year,city) the list of couples (month,days clear counts)
                .mapToPair(x->new Tuple2<>(new Tuple2<>(x._1()._2(),x._1()._3()),new Tuple2<>(x._1()._1(),x._2())))

                //Filtering the previous couples which do not have at least 15 clear days for the month specified in the value
                //and grouping them by (year,city) key
                .filter(x->x._2()._2()>=15)
                .groupByKey();


        //filtering the new couples which do not contains three elements(one for each month specified by the query) in the value field
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>> clearCityWithYear=clearMonthsForCityAndForYear.filter(x-> Iterables.size(x._2())==3);


        JavaPairRDD<String,Iterable<String>> finalResult= clearCityWithYear
                //mapping the previous cuples to new key-value pairs where the year represents the key and the value is the city
                //and grouping them by key, sorting by year
                .mapToPair(x->new Tuple2<>(x._1()._1(),x._1()._2()))
                //sorting by key is necessary for later saving on (year field will be part of the row key)
                .groupByKey().sortByKey();

        //constructing final rdd for saving, the first tuple field is used to construct a unique row key for HBase saving
        JavaRDD<Tuple3<String,String,String>> RDDForSaving = finalResult.flatMap((FlatMapFunction<Tuple2<String, Iterable<String>>, Tuple3<String, String, String>>) t -> {
            ArrayList<Tuple3<String,String,String>> arrFinal= new ArrayList<>();
            t._2().forEach(x->arrFinal.add(new Tuple3<>(Integer.toString(Lists.newArrayList(t._2().iterator()).indexOf(x)+1),t._1(),x)));

            return arrFinal.iterator();
        });

        //Saving output as
        //SaveOutput s=new SaveOutput();
        //s.saveOutputQuery1(RDDForSaving,sparkSession,pathToHDFS);


        for (int i=0; i< RDDForSaving.collect().size();i++){
            System.out.println("ANNO: "+ RDDForSaving.collect().get(i));
        }

        sc.stop();

    }



}

