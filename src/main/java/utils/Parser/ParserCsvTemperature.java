package utils.Parser;

import com.google.common.collect.Lists;
import entities.TemperatureMeasurement;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.ArrayList;

public class ParserCsvTemperature {


    public static Tuple2<JavaRDD<TemperatureMeasurement>,Long> construct_CleanRDD(JavaSparkContext sc,String pathToFileTemperature){

        Long timepreprocessing=System.currentTimeMillis();

        //creating temperature rdd

        JavaRDD<String> initialtemperature= sc.textFile(pathToFileTemperature/*"hdfs://localhost:54310/data/temperature.csv"*/);
        String headerCityList=initialtemperature.first();
        String [] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<String> initialRDD = initialtemperature.filter(x->!x.equals(headerCityList));


        JavaRDD<TemperatureMeasurement> temperatureInitial =initialRDD.flatMap((FlatMapFunction<String, TemperatureMeasurement>) s -> {
            String cvsSplitBy = ",";

            ArrayList<TemperatureMeasurement> temperatureMeasurements = new ArrayList<>();
            String[] measurements = s.split(cvsSplitBy,-1);

            Lists.newArrayList(cityList).forEach(city-> temperatureMeasurements.add(new TemperatureMeasurement(city,measurements[0],
                    measurements[ Lists.newArrayList(cityList).indexOf(city)+1])));

            return temperatureMeasurements.iterator();
        });



        //filtering missign values and reconstructing malformed ones
        JavaRDD<TemperatureMeasurement> temperatureFiltered=temperatureInitial.filter(x->(!x.getTemperature().equals("")
                && !x.getDate().equals("")));

        JavaRDD<TemperatureMeasurement> temperatureRDDclean=temperatureFiltered.map((Function<TemperatureMeasurement, TemperatureMeasurement>) t -> {
            String temperature= t.getTemperature();
            if (!t.getTemperature().contains(".")){
                t.setTemperature(ParserCsvTemperature.fixBadValues(temperature));
            }
            return t ;
        });

        timepreprocessing=System.currentTimeMillis()-timepreprocessing;

        return new Tuple2<>(temperatureRDDclean,timepreprocessing);

    }



    private static String fixBadValues (String value){

        BigDecimal val= new BigDecimal(value).movePointLeft(value.length()-3);

        return val.toString();

    }


}
