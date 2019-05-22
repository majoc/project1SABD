package utils.Parser;

import com.google.common.collect.Lists;
import entities.HumidityMeasurement;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;

public class ParserCleanerHumidity {

    public static Tuple2<JavaRDD<HumidityMeasurement>,Long> construct_cleanRDD(JavaSparkContext sc,String pathToFileHumidity) {

        Long processingTime=System.currentTimeMillis();

        //creating humidity rdd
        JavaRDD<String> initialhumidity= sc.textFile(pathToFileHumidity/*"hdfs://localhost:54310/data/humidity.csv"*/);
        String headerCityList=initialhumidity.first();
        String[] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<String> initialHumidity= initialhumidity.filter(x->!x.equals(headerCityList));

        JavaRDD<HumidityMeasurement> humidityMeasurements=initialHumidity.flatMap((FlatMapFunction<String, HumidityMeasurement>) s -> {
            String cvsSplitBy = ",";

            ArrayList<HumidityMeasurement> humMeasurements = new ArrayList<>();
            String[] measurements = s.split(cvsSplitBy,-1);

            Lists.newArrayList(cityList).forEach(city-> humMeasurements.add(new HumidityMeasurement(city,measurements[0],
                    measurements[ Lists.newArrayList(cityList).indexOf(city)+1])));

            return humMeasurements.iterator();
        });


        //filter instances with missing humidity values or malformed

        JavaRDD<HumidityMeasurement> filtered= humidityMeasurements.filter(x->(!x.getHumidity().equals("")
                && !x.getDate().equals("")));

        return new Tuple2<>(filtered,System.currentTimeMillis()-processingTime);
    }
}
