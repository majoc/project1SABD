package queries;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.WeatherMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import utils.*;

import utils.Parser.BuilderCleanerConditionRDD;


import java.util.ArrayList;



public class Query1 {



    public static void query1(JavaSparkContext jsc, JavaRDD<CityInfo> cityRDD,Long preprocessingTime, SparkSession session, String pathToHDFS, String pathToFileCondition, String pathToFileCities) {


        Long processingTime= System.currentTimeMillis();


        Tuple2<JavaRDD<WeatherMeasurement>,Long> cleaned= BuilderCleanerConditionRDD.construct_cleanRDD(jsc,cityRDD,pathToFileCities,pathToFileCondition);

        //getting cleaning time
        Long cleaningTime= cleaned._2()+preprocessingTime;



        //Getting all info in the tuple and implementing a word count based on (day,month,year,city,condition) keys,
        // which basically counts
        //the number of hours per day characterized by the key specified weather_condition
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcountSkyIsClear= cleaned._1().mapToPair(x->
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
        SaveOutput s=new SaveOutput();
        s.saveOutputQuery1(RDDForSaving,session,pathToHDFS);




        processingTime=System.currentTimeMillis()-processingTime;
        Tuple2 tupleTime=new Tuple2<>(processingTime,cleaningTime);

        ArrayList<Tuple2<Long,Long>> performance= new ArrayList<>();
        performance.add(tupleTime);

        JavaRDD<Tuple2<Long,Long>> perfTime=jsc.parallelize(performance);

        s.saveTimes(perfTime,session,pathToHDFS,"times1.csv");




    }



}

