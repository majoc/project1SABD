package utils.Parser;

import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.WeatherMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;
import utils.ConvertDatetime;

import java.util.ArrayList;

public class ParserCleanerCondition {
    public static Tuple2<JavaRDD<WeatherMeasurement>,Long> construct_cleanRDD(JavaSparkContext sc, String pathcities, String pathcondition){

        Long preprocessingTime= System.currentTimeMillis();

        //creating cityRDD
        JavaRDD<String> initialcity= sc.textFile(pathcities/*"hdfs://localhost:54310/data/city_attributes.csv.COMPLETED"*/);
        String header=initialcity.first();


        //filtering content except header line
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));

        //creating a city object RDD
        JavaRDD<CityInfo> cityInfo= initialCityCleaned.map((Function<String, CityInfo>)
                s -> ParserCsvCity.parseLine(s,"query1"));


        //creating weather_condition rdd
        JavaRDD<String> initialweather= sc.textFile(pathcondition/*"hdfs://localhost:54310/data/temperature.csv"*/);

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


        return new Tuple2<>(measuresConverted,System.currentTimeMillis()-preprocessingTime);
    }
}
