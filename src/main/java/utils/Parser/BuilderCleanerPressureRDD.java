package utils.Parser;

import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.PressureMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import utils.ConvertDatetime;

import java.util.ArrayList;

public class BuilderCleanerPressureRDD {

    public static Tuple2<JavaPairRDD<String,Tuple2<PressureMeasurement,CityInfo>>,Long> construct_cleanRDD(JavaSparkContext sc, String pathToFilePressure, JavaRDD<CityInfo> cities) {

        Long processingTime=System.currentTimeMillis();

        //creating pressure rdd
        JavaRDD<String> initialpressure= sc.textFile(pathToFilePressure/*"hdfs://localhost:54310/data/pressure.csv"*/);
        String headerCityList=initialpressure.first();
        String[] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<String> initialPressure = initialpressure.filter(x->!x.equals(headerCityList));

        JavaRDD<PressureMeasurement> pressureMeasurements=initialPressure.flatMap((FlatMapFunction<String, PressureMeasurement>) s -> {
            String cvsSplitBy = ",";

            ArrayList<PressureMeasurement> pressMeasurements = new ArrayList<>();
            String[] measurements = s.split(cvsSplitBy,-1);

            Lists.newArrayList(cityList).forEach(city-> pressMeasurements.add(new PressureMeasurement(city,measurements[0],
                    measurements[ Lists.newArrayList(cityList).indexOf(city)+1])));

            return pressMeasurements.iterator();
        });

        //filter instances with missing pressure values or malformed
        JavaRDD<PressureMeasurement> pressureRDDclean=pressureMeasurements.filter(x->(!x.getPressure().equals("")
                && !x.getDate().equals("")));

        //rdds for joining city info(included nation) with measurement instance
        JavaPairRDD<String,PressureMeasurement> cityPressures = pressureRDDclean.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String, CityInfo> cityCityInfo = cities.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        //final couples-->((nation,year,month),(value,value^2,count,value,value)
        JavaPairRDD<String,Tuple2<PressureMeasurement,CityInfo>> statRDDP =
                //performing an inner join between measurement rdd  and city info rdd (containing nation information)
                cityPressures.join(cityCityInfo)

                        //mapping previous RDD in a new one with converted DateTime and only query relevant info

                        .mapToPair(x->new Tuple2<>(x._1(),
                                new Tuple2<>(new PressureMeasurement(x._2()._1().getCity(), ConvertDatetime.convert(x._2()._2().getTimezone(),x._2()._1().getDate()),x._2()._1().getPressure()),x._2()._2())));


        return new Tuple2<>(statRDDP,System.currentTimeMillis()-processingTime);
    }
}
