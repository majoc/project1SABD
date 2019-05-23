package utils.Parser;

import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.TemperatureMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import utils.ConvertDatetime;

import java.math.BigDecimal;
import java.util.ArrayList;

public class BuilderCleanerTemperatureRDD {


    public static Tuple2<JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>>,Long> construct_CleanRDD(JavaSparkContext sc, String pathToFileTemperature, JavaRDD<CityInfo> cities){

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
                t.setTemperature(BuilderCleanerTemperatureRDD.fixBadValues(temperature));
            }
            return t ;
        });

        //rdds for joining city info(included nation) with measurement instance
        JavaPairRDD<String,TemperatureMeasurement> cityTemperatures = temperatureRDDclean.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String,CityInfo> cityCityInfo = cities.mapToPair(x -> new Tuple2<>(x.getCityName(),x));



        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> statRDD=
                //performing an inner join between measurement rdd  and city info rdd (containing nation information)
                cityTemperatures.join(cityCityInfo)


                        //mapping previous RDD in a new one with converted DateTime and only query relevant info
                        .mapToPair(x->new Tuple2<>(x._1(),
                                new Tuple2<>(new TemperatureMeasurement(x._2()._1().getCity(),
                                        ConvertDatetime.convert(x._2()._2().getTimezone(),
                                        x._2()._1().getDate()),
                                        x._2()._1().getTemperature()),
                                        x._2()._2())));



        System.out.println("DENTRO CREAZIONE RDD");


        return new Tuple2<>(statRDD,System.currentTimeMillis()- timepreprocessing);

    }



    private static String fixBadValues (String value){

        BigDecimal val= new BigDecimal(value).movePointLeft(value.length()-3);

        return val.toString();

    }


}
