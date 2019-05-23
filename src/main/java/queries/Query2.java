package queries;


import entities.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import utils.ConvertDatetime;
import utils.Parser.*;
import utils.SaveOutput;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

public class Query2 {



    private static Long cleaningTime=0L;

    public static void query2(JavaSparkContext sc, SparkSession sparkSession,JavaRDD<CityInfo> cities,JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> temp,Long timePartial, String pathHDFS, String pathHumidity, String pathPressure) {

        //System.setProperty("hadoop.home.dir","C:\\winutils");



        Long processingTime= System.currentTimeMillis();


        //creating cityRDD  "hdfs://localhost:54310/data/city_attributes.csv.COMPLETED"


        cleaningTime+=timePartial;

        //computing temperature statistics
        temperatureStatistics(sparkSession,temp,pathHDFS);


        //computing humidity statistics
        humidityStatistics(cities, sc,sparkSession, pathHDFS, pathHumidity);


        //computing pressure statistics
        pressureStatistics(cities,sc,sparkSession,pathHDFS, pathPressure);



        processingTime=System.currentTimeMillis()-processingTime;
        SaveOutput s=new SaveOutput();
        Tuple2 tupleTime=new Tuple2<>(processingTime,cleaningTime);

        ArrayList<Tuple2<Long,Long>> performance= new ArrayList<>();
        performance.add(tupleTime);

        JavaRDD<Tuple2<Long,Long>> perfTime=sc.parallelize(performance);

        s.saveTimes(perfTime,sparkSession,pathHDFS, "times2.csv");


        System.out.println("TEMPO PROCESSAMENTO (ms) " +processingTime+ "  TEMPO PREPROCESSING (ms) " + cleaningTime);






    }


    private static void temperatureStatistics(SparkSession sparkSession,JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> temperatures, String pathHDFS){




        //Generating couples with key equal to (nation,year,month) and value equal to (measure value, measure square value, 1, measure value, measure value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statPini=temperatures.mapToPair(x-> new Tuple2<>(new Tuple3<>(x._2()._2().getNation(),x._2()._1().getYear(),x._2()._1().getMonth())
                ,new Tuple5<>(Double.parseDouble(x._2()._1().getTemperature()), Math.pow(Double.parseDouble(x._2()._1().getTemperature()),2),1,
                Double.parseDouble(x._2()._1().getTemperature()),Double.parseDouble(x._2()._1().getTemperature()))));


        //following reduce operation are all commutative and associative
        //compute sum of temperature values, square temperature values (for std deviation computing), count of occurrences, min and max of temperature values
        //reducing by key-->couples (nation,year,month) , (sum values, square sum value,count, min value, max value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statPartial= statPini
                .reduceByKey((Function2<Tuple5<Double, Double, Integer, Double, Double>, Tuple5<Double, Double, Integer, Double, Double>, Tuple5<Double, Double, Integer, Double, Double>>)
                        (t1, t2) -> new  Tuple5<> (t1._1()+t2._1(),t1._2() + t2._2(),t1._3()+t2._3(),Math.min(t1._4(),t2._4()),Math.max(t1._5(),t2._5())));


        //compute, mean, sum of square values divided for count occurrences, min and max of temperature value
        //couples (nation,year,month) , (mean, square sum value/count, min value, max value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> RDDFinal=statPartial
                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(x._2()._1()/x._2()._3(), //mean
                                x._2()._2()/x._2()._3(), //squared sum/count
                                x._2()._4(),  //min
                                x._2()._5()   //max
                        )
                )
                )

                //finally computing all requested stat
                //couples (nation,year,month) , (mean, std dev, min value, max value)
                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(BigDecimal.valueOf(x._2()._1()).setScale(5,RoundingMode.HALF_UP).doubleValue(), //mean
                                BigDecimal.valueOf(Math.sqrt(x._2()._2()-Math.pow(x._2()._1(),2))).setScale(5,RoundingMode.HALF_UP).doubleValue(), //stddev
                                x._2()._3(), //min
                                x._2()._4()))).cache(); //max


        SaveOutput s=new SaveOutput();
        s.saveOutputQuery2(RDDFinal,sparkSession,pathHDFS,"/output2/output2_temperature.csv");




    }

    private static void humidityStatistics(JavaRDD<CityInfo> cityRDD, JavaSparkContext sc,SparkSession sparkSession,String pathToHDFS, String path){


        Tuple2<JavaPairRDD<String,Tuple2<HumidityMeasurement,CityInfo>>,Long> humidities=ParserCleanerHumidity.construct_cleanRDD(sc,path, cityRDD);

        //getting cleaning time
        cleaningTime+= humidities._2();




              //Generating couples with key equal to (nation,year,month) and value equal to (measure value, measure square value, 1, measure value, measure value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statRDDInitial= humidities._1().mapToPair(x-> new Tuple2<>(new Tuple3<>(x._2()._2().getNation(),x._2()._1().getYear(),x._2()._1().getMonth()),
                        new Tuple5<>(Double.parseDouble(x._2()._1().getHumidity()),
                                Math.pow(Double.parseDouble(x._2()._1().getHumidity()),2),1,
                                Double.parseDouble(x._2()._1().getHumidity()),
                                Double.parseDouble(x._2()._1().getHumidity()))));


        //following reduce operation are all commutative and associative
        //reducing by key-->couples (nation,year,month) , (sum values, square sum value,count, min value, max value)
        //compute sum of humidity values, square humidity values (for std deviation computing), count of occurrences, min and max of humidity values
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statPartial= statRDDInitial
                .reduceByKey((Function2<Tuple5<Double, Double, Integer, Double, Double>,
                        Tuple5<Double, Double, Integer, Double, Double>,
                        Tuple5<Double, Double, Integer, Double, Double>>)
                        (t1, t2) -> new  Tuple5<> (t1._1()+t2._1(),
                                t1._2() + t2._2(), //sum
                                t1._3()+t2._3(),   //squared sum
                                Math.min(t1._4(),t2._4()), //min
                                Math.max(t1._5(),t2._5()))); //max


        //compute, mean, sum of square values divided for count occurrences, min and max of humidity value
        //couples (nation,year,month) , (mean, square sum value/count, min value, max value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> RDDFinal=statPartial

                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(x._2()._1()/x._2()._3(),x._2()._2()/x._2()._3(),x._2()._4(),x._2()._5())))

                //finally computing all requested stat
                //couples (nation,year,month) , (mean, std dev, min value, max value)
                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(BigDecimal.valueOf(x._2()._1()).setScale(5,RoundingMode.HALF_UP).doubleValue(),
                                BigDecimal.valueOf(Math.sqrt(x._2()._2()-Math.pow(x._2()._1(),2))).setScale(5, RoundingMode.HALF_UP).doubleValue(),
                                x._2()._3(),
                                x._2()._4()))).cache();



        SaveOutput s=new SaveOutput();
        s.saveOutputQuery2(RDDFinal,sparkSession,pathToHDFS,"/output2/output2_humidity.csv");



    }

    private static void pressureStatistics(JavaRDD<CityInfo> cityRDD, JavaSparkContext sc,SparkSession sparkSession,String pathToHDFS, String pathToFilePressure) {


        Tuple2<JavaPairRDD<String,Tuple2<PressureMeasurement,CityInfo>>,Long> pressures=ParserCleanerPressure.construct_cleanRDD(sc,pathToFilePressure,cityRDD);

        //getting cleaning time
        cleaningTime+= pressures._2();



                 //Generating couples with key equal to (nation,year,month) and value equal to (measure value, measure square value, 1, measure value, measure value)
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statRDDInitial= pressures._1().mapToPair(x-> new Tuple2<>(new Tuple3<>(x._2()._2().getNation(),x._2()._1().getYear(),x._2()._1().getMonth())
                ,new Tuple5<>(Double.parseDouble(x._2()._1().getPressure()), Math.pow(Double.parseDouble(x._2()._1().getPressure()),2),1,
                Double.parseDouble(x._2()._1().getPressure()),Double.parseDouble(x._2()._1().getPressure()))));



        //following reduce operation are all commutative and associative
        //reducing by key-->couples (nation,year,month) , (sum values, square sum value,count, min value, max value)
        //compute sum of pressure values, square pressure values (for std deviation computing), count of occurrences, min and max of pressure values
        JavaPairRDD<Tuple3<String,String,String>, Tuple5<Double,Double,Integer,Double,Double>> statPartial= statRDDInitial
                .reduceByKey((Function2<Tuple5<Double, Double, Integer, Double, Double>,
                        Tuple5<Double, Double, Integer, Double, Double>,
                        Tuple5<Double, Double, Integer, Double, Double>>)
                        (t1, t2) -> new  Tuple5<> (t1._1()+t2._1(),
                                t1._2() + t2._2(),  //sum
                                t1._3()+t2._3(),    //squared sum
                                Math.min(t1._4(),t2._4()), //min
                                Math.max(t1._5(),t2._5()))); //max

        //couples (nation,year,month) , (mean, square sum value/count, min value, max value)
        //compute, mean, sum of square values divided for count occurrences, min and max of pressure value
        JavaPairRDD<Tuple3<String,String,String>, Tuple4<Double,Double,Double,Double>> RDDFinal=statPartial
                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(x._2()._1()/x._2()._3(), //mean
                                x._2()._2()/x._2()._3(), //squares sum /count
                                x._2()._4(), //min
                                x._2()._5()))) //max


                //finally computing all requested stat
                //couples (nation,year,month) , (mean, std dev, min value, max value)
                .mapToPair(x-> new Tuple2<>(x._1(),
                        new Tuple4<>(BigDecimal.valueOf(x._2()._1()).setScale(5,RoundingMode.HALF_UP).doubleValue(), //mean
                                BigDecimal.valueOf(Math.sqrt(x._2()._2()-Math.pow(x._2()._1(),2))).setScale(5, RoundingMode.HALF_UP).doubleValue(), //stddev
                                x._2()._3(), //min
                                x._2()._4()))); //max



        SaveOutput s=new SaveOutput();
        s.saveOutputQuery2(RDDFinal,sparkSession,pathToHDFS,"/output2/output2_pressure.csv");




    }


}
