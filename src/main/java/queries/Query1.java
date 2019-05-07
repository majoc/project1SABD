package queries;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import utils.WeatherMeasurement;
import utils.ParserCsv;

import java.util.ArrayList;


public class Query1 {

    private static String pathToFile = "data/prj1_dataset/weather_description.csv";

    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ArrayList<WeatherMeasurement> measurements =ParserCsv.parseCSV(pathToFile);
        JavaRDD<WeatherMeasurement> w_measurements=sc.parallelize(measurements);

        //filter the null elements and the elements which are not interesting for this query
        JavaRDD<WeatherMeasurement> w_meas_notNull=w_measurements.filter(x->x!=null && (x.getMonth().equals("3")||x.getMonth().equals("4")||x.getMonth().equals("5"))).cache();

        //Getting all info in the tuple and implementing a word count based on (day,year,month,city) keys, which basically counts
        //the number of hours per day carachterized by the key specified weather_condition
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYear= w_meas_notNull.mapToPair(x->new Tuple2<>(new Tuple5<String,String,String,String,String>(x.getDay(),x.getMonth(),x.getYear(),x.getCity(),x.getWeather_condition()),1));
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcount= citiesPerYear.reduceByKey((x,y)->x+y);

        //Filtering couples where weather condition is "sky is clear" and the number of hours per day is greater than 16
        JavaPairRDD<Tuple5<String,String,String,String,String>,Integer> citiesPerYearcountClear= citiesPerYearcount.filter(x->x._1()._5().equals("sky is clear") && x._2()>=16);

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

        ArrayList<Tuple2<String,Iterable<String>>> output=Lists.newArrayList(finalResult.collect());

        for (int i=0; i<output.size();i++){
            System.out.println("ANNO: "+output.get(i)._1()+"  LISTA CITTA :"+ output.get(i)._2());
        }





    }



}
