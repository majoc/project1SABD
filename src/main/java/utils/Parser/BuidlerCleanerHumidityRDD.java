package utils.Parser;

import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.HumidityMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import utils.ConvertDatetime;

import java.util.ArrayList;

public class BuidlerCleanerHumidityRDD {

    public static Tuple2<JavaPairRDD<String,Tuple2<HumidityMeasurement,CityInfo>>,Long> construct_cleanRDD(JavaSparkContext sc,String pathToFileHumidity, JavaRDD<CityInfo> cities) {

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
                && !x.getDate().equals("") && !(Double.parseDouble(x.getHumidity()) < 0.0)) && !(Double.parseDouble(x.getHumidity()) > 100.0));


        //rdds for joining city info(included nation) with measurement instance
        JavaPairRDD<String,HumidityMeasurement> cityHumidities = filtered.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String, CityInfo> cityCityInfo = cities.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        //final couples-->((nation,year,month),(value,value^2,count,value,value)
        JavaPairRDD<String,Tuple2<HumidityMeasurement,CityInfo>> statRDDP=
                //performing an inner join between measurement rdd  and city info rdd (containing nation information)
                cityHumidities.join(cityCityInfo)

                        //mapping previous RDD in a new one with converted DateTime and only query relevant info
                        .mapToPair(x->new Tuple2<>(x._1(),
                                new Tuple2<>
                                        (new HumidityMeasurement(x._2()._1().getCity(),
                                                ConvertDatetime.convert(x._2()._2().getTimezone(),
                                                        x._2()._1().getDate()),x._2()._1().getHumidity()),x._2()._2())));

        return new Tuple2<>(statRDDP,System.currentTimeMillis()-processingTime);
    }
}
