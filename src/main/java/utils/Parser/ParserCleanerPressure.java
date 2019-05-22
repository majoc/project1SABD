package utils.Parser;

import com.google.common.collect.Lists;
import entities.PressureMeasurement;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;

public class ParserCleanerPressure {

    public static Tuple2<JavaRDD<PressureMeasurement>,Long> construct_cleanRDD(JavaSparkContext sc, String pathToFilePressure) {

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

        return new Tuple2<>(pressureRDDclean,System.currentTimeMillis()-processingTime);
    }
}
