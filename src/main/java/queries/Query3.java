package queries;



import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import entities.CityInfo;
import entities.TemperatureMeasurement;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utils.SaveOutput;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;


public class Query3 {


    private static String year1 = "2016";
    private static String year2 = "2017";


    public static void query3(JavaSparkContext sc, SparkSession sparkSession, JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> temperatures, String pathToHDFS) {

        //System.setProperty("hadoop.home.dir","C:\\winutils");


        top3cityMaxDifference(sc, sparkSession,temperatures,pathToHDFS);




    }


    public static void top3cityMaxDifference( JavaSparkContext sc, SparkSession sparkSession,JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> convertedJoin, String pathToHDFS) {

        Long processingTime= System.currentTimeMillis();


        //getting cleaning time
        Long cleaningTime= 0L;


        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureFirstGroup = temperatureMean(convertedJoin,"6","7","8","9");

        //temperatureFirstGroup.saveAsTextFile("prove3");

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureSecondGroup = temperatureMean(convertedJoin,"1","2","3","4");

        //temperatureFirstGroup.saveAsTextFile("prove4");

        //joining the groups value so that we can later do difference
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Double>>> temperatureDiffGrouped= temperatureFirstGroup
                //performing th inner join based on the triple key (nation, year,city)
                .join(temperatureSecondGroup)

                //mapping into couples ((nation,year,city), difference)
                .mapToPair(x->new Tuple2<>(x._1(),x._2()._1()-x._2()._2()))

                //exchanging previous key and value and then sorting by key, in order to
                //do subsequent chart (global sorting implies that any subset is also internally sorted)
                .mapToPair(x-> new Tuple2<>(x._2(),x._1())).sortByKey(false)

                //mapping into couples ((nation,year),(city,difference))
                .mapToPair(x-> new Tuple2<>(
                        new Tuple2 <>(x._2()._1(),x._2()._2()),
                        new Tuple2<>(x._2()._3(),x._1())))

                //grouping all by (nation, year) and caching for subsequent usage
                .groupByKey().cache();

        //temperatureDiffGrouped.saveAsTextFile("prove5");

        //top 3 chart for 2017
        JavaPairRDD<Tuple2<String,String>, ArrayList<Tuple2<String,Double>>> intermediateSecond=temperatureDiffGrouped

                //separating the subset relative to year2--> it's still sorted
                .filter(x->x._1()._2().equals(year2))

                //selecting the first three element (city,difference) in the iterable element
                //resulting by the group by on the (nation, year) key. This will result in
                //couples, one for each nation, (nation,year2) [top3value element (city, difference)]
                .mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<String, Double>>>,
                    Tuple2<String, String>,
                    Iterable<Tuple2<String, Double>>>)
                    t -> new Tuple2<>(t._1(), Iterables.limit(t._2(),3)))

                //mapping for easier index manipulation
                .mapToPair(x-> new Tuple2<>(x._1(),Lists.newArrayList(x._2().iterator())));


        //top chart of all cities in 2016
        JavaPairRDD<Tuple2<String,String>, ArrayList<Tuple2<String,Double>>> intermediateFirst=temperatureDiffGrouped

                //separating the subset relative to year1--> it's still sorted
                .filter(x->x._1()._2().equals(year1))

                //this mapping is used to convert the sorted iterable list, relative to year1,
                //into an array list in order to make easier subsequent index manipulation
                .mapToPair(x-> new Tuple2<>(x._1(),Lists.newArrayList(x._2().iterator())));




        //generating couples ((city,nation),(position in chart, chart year=2017, temperature_value))
        //for each element of the previous RDD generates one element for each element of the iterable
        //with the structure indicated above
        JavaPairRDD<Tuple2<String,String>, Tuple3<Integer,String,Double>> temperatureSecondYear= intermediateSecond
                .flatMapToPair((PairFlatMapFunction<Tuple2<Tuple2<String, String>,
                        ArrayList<Tuple2<String, Double>>>,
                        Tuple2<String, String>, Tuple3<Integer, String, Double>>)
                        t -> {
            ArrayList<Tuple2<Tuple2<String,String>, Tuple3<Integer, String, Double>>> arr=new ArrayList<>();
            t._2().forEach(x-> arr.add(new Tuple2<Tuple2<String,String>,
                    Tuple3<Integer, String, Double>>(
                            new Tuple2<>(x._1(), t._1()._1()),
                    new Tuple3<>(t._2().indexOf(x)+1,t._1()._2(),
                            BigDecimal.valueOf(x._2()).setScale(5, RoundingMode.HALF_UP).doubleValue()))));


            return arr.listIterator();
        });

        //generating couples ((city,nation),(position in chart, chart year=2016, temperature_value))
        //for each element of the previous RDD generates one element for each element of the iterable
        //with the structure indicated above
        JavaPairRDD<Tuple2<String,String>, Tuple3<Integer,String,Double>> temperatureFirstYear= intermediateFirst
                .flatMapToPair((PairFlatMapFunction<Tuple2<Tuple2<String,
                        String>, ArrayList<Tuple2<String, Double>>>,
                        Tuple2<String, String>, Tuple3<Integer, String, Double>>)
                        t -> {
            ArrayList<Tuple2<Tuple2<String,String>, Tuple3<Integer, String, Double>>> arr=new ArrayList<>();
            t._2().forEach(x-> arr.add(new Tuple2<Tuple2<String,String>, Tuple3<Integer, String, Double>>
                    (new Tuple2<>(x._1(), t._1()._1()),new Tuple3<>(t._2().indexOf(x)+1,t._1()._2(),
                            BigDecimal.valueOf(x._2()).setScale(5, RoundingMode.HALF_UP).doubleValue()))));


            return arr.listIterator();
        });


        //join based on previous keys (city,nation)
        //producing rdd elements (city,nation)(position2,year2, diff_value2) (position1,year1,diff_value1)
        JavaPairRDD<Tuple2<String,String>, Tuple2<Tuple3<Integer,String,Double>,Tuple3<Integer,String,Double>>> finalRDD=
                temperatureSecondYear.join(temperatureFirstYear);


        SaveOutput s=new SaveOutput();
        s.saveOutputQuery3(finalRDD,sparkSession,pathToHDFS);

        processingTime=System.currentTimeMillis()-processingTime;
        Tuple2 tupleTime=new Tuple2<>(processingTime,cleaningTime);

        ArrayList<Tuple2<Long,Long>> performance= new ArrayList<>();
        performance.add(tupleTime);

        JavaRDD<Tuple2<Long,Long>> perfTime=sc.parallelize(performance);

        s.saveTimes(perfTime,sparkSession,pathToHDFS,"times3.csv");





    }

    private static JavaPairRDD<Tuple3<String,String,String>,Double> temperatureMean(JavaPairRDD<String, Tuple2<TemperatureMeasurement, CityInfo>> convertedJoin, String m1, String m2, String m3, String m4) {

        //operates required filtering
        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> temperatureFiltered = convertedJoin
                .filter(x->(x._2()._1().getYear().equals(year1) ||
                x._2()._1().getYear().equals(year2)) && (x._2()._1().getMonth().equals(m1) ||
                x._2()._1().getMonth().equals(m2) || x._2()._1().getMonth().equals(m3) ||
                x._2()._1().getMonth().equals(m4)) &&
                ( Integer.parseInt(x._2()._1().getHour()) <= 15 && Integer.parseInt(x._2()._1().getHour()) >= 12));

        //implementing temperature value aggregation by sum
        // in which key is (nation,year,city) and counting total occurrences
        JavaPairRDD<Tuple3<String,String,String>,Tuple2<Double,Integer>> temperatureByKey= temperatureFiltered
                .mapToPair(x->new Tuple2<>(
                        new Tuple3<>(x._2()._2().getNation(),
                                x._2()._1().getYear(),
                                x._2()._1().getCity()),
                        new Tuple2<>(Double.parseDouble(x._2()._1().getTemperature()),1)))

                //aggregation
                .reduceByKey((Function2<Tuple2<Double, Integer>,
                        Tuple2<Double, Integer>,
                        Tuple2<Double, Integer>>)
                        (t1, t2) -> new Tuple2<>(t1._1()+t2._1(),t1._2()+t2._2()));

        //return an RDD in which key is the triple (nation, year, city) and the value is the temperature mean
        return temperatureByKey.mapToPair(x->new Tuple2<>(x._1(),x._2()._1()/x._2()._2())).coalesce(1);



    }



}
