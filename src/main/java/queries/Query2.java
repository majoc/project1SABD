package queries;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import utils.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class Query2 {

    private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";

    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ArrayList<TemperatureMeasurement> temperature = ParserCsvTemperature.parseCSV(pathToFileTemperature);

        ArrayList<CityInfo> citiesArray = ParserCsvCity.parseCSV(pathToFileCities,"query2");

        JavaRDD<TemperatureMeasurement> temperatureRDD =sc.parallelize(temperature);

        //filter instances with missing temperature values
        JavaRDD<TemperatureMeasurement> temperatureRDDclean = temperatureRDD.filter(x->x!=null );

        //creating rdd containing citiInfo instances with corresponding nation
        JavaRDD<CityInfo> cityRDD = sc.parallelize(citiesArray).cache();

        JavaPairRDD<String,TemperatureMeasurement> cityTemperatures = temperatureRDDclean.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String,CityInfo> cityCityInfo = cityRDD.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> joinRDD = cityTemperatures.join(cityCityInfo);

        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statRDDInitial= joinRDD.mapToPair(x-> new Tuple2<>(new Tuple3<>(x._2()._2().getNation(),x._2()._1().getYear(),x._2()._1().getMonth())
                ,new Tuple5<>(Double.parseDouble(x._2()._1().getTemperature()), Math.pow(Double.parseDouble(x._2()._1().getTemperature()),2),1,
                Double.parseDouble(x._2()._1().getTemperature()),Double.parseDouble(x._2()._1().getTemperature()))));



        //compute sum of temperature values, square temperature values (for std deviation computing), count of occurrences, min and max of temperature values
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statPartial= statRDDInitial.reduceByKey(new Function2<Tuple5<Double, Double, Integer, Double, Double>, Tuple5<Double, Double, Integer, Double, Double>, Tuple5<Double, Double, Integer, Double, Double>>() {
            @Override
            public Tuple5<Double, Double, Integer, Double, Double> call(Tuple5<Double, Double, Integer, Double, Double> t1, Tuple5<Double, Double, Integer, Double, Double> t2) throws Exception {

                return new  Tuple5<> (t1._1()+t2._1(),t1._2() + t2._2(),t1._3()+t2._3(),Math.min(t1._4(),t2._4()),Math.max(t1._5(),t2._5()));


            }
        });


        //compute, mean, sum of square values divided for count occurrences, min and max of temperature value
        JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> statPartial2=statPartial.mapToPair(x-> new Tuple2<>(x._1(),new Tuple4<>(x._2()._1()/x._2()._3(),x._2()._2()/x._2()._3(),x._2()._4(),x._2()._5())));

        JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> statFinal=statPartial2.mapToPair(x-> new Tuple2<>(x._1(),new Tuple4<>(x._2()._1(),Math.sqrt(x._2()._2()-Math.pow(x._2()._1(),2)),x._2()._3(),x._2()._4())));

        JavaPairRDD<String, Tuple2<Tuple2<String,String>,Tuple4<Double,Double,Double,Double>>> outputRDD= statFinal.mapToPair(x-> new Tuple2<>(x._1()._1(), new Tuple2<>(
                new Tuple2<>(x._1()._2(),x._1()._3()),new Tuple4<>(BigDecimal.valueOf(x._2()._1()).setScale(3,RoundingMode.HALF_UP).doubleValue()
                ,BigDecimal.valueOf(x._2()._2()).setScale(3,RoundingMode.HALF_UP).doubleValue()
                ,x._2()._3(),x._2()._4()))));

        //JavaPairRDD<String, Iterable<Tuple2<Tuple2<String,String>,Tuple4<Double,Double,Double,Double>>>> output= outputRDD.groupByKey();

        ArrayList<Tuple2<String, Tuple2<Tuple2<String,String>,Tuple4<Double,Double,Double,Double>>>> outputPrint=Lists.newArrayList(outputRDD.collect());

        for (int i=0; i<outputPrint.size();i++){
            System.out.println("NAZIONE: "+outputPrint.get(i)._1()+"  LISTA CITTA :"+ outputPrint.get(i)._2());
        }



        sc.stop();

    }




}
