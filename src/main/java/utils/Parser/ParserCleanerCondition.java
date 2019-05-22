package utils.Parser;

import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.WeatherMeasurement;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

import java.util.ArrayList;

public class ParserCleanerCondition {
    public static Tuple3<JavaRDD<WeatherMeasurement>,JavaRDD<CityInfo>,Long> construct_cleanRDD(JavaSparkContext sc, String pathcities, String pathcondition){

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


        return new Tuple3<>(w_meas_notNull,cityInfo,System.currentTimeMillis()-preprocessingTime);
    }
}
