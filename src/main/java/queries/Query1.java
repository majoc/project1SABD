package queries;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import utils.WeatherMeasurement;
import utils.ParserCsv;

import java.util.ArrayList;


public class Query1 {

    private static String pathToFile = "data/prj1_dataset/weather_description.csv";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ArrayList<WeatherMeasurement> measurements =ParserCsv.parseCSV(pathToFile);
        JavaRDD<WeatherMeasurement> w_measurements=sc.parallelize(measurements);

        JavaRDD<WeatherMeasurement> w_meas_notNull=w_measurements.filter(x->x!=null);
        JavaRDD<Tuple4<String,String,String,String>> citiesPerYear= w_meas_notNull.map(x->new Tuple4<String,String,String,String>(((Integer)x.getMonth()).toString(),x.getYear(),x.getCity(),x.getWeather_condition()));
        JavaPairRDD<Tuple3<String,String,String>,String> citiesPerYear2 = citiesPerYear.filter(x-> (x._1().equals("3") || x._1().equals("4") ||x._1().equals("5")))
                .mapToPair(x-> new Tuple2<>(new Tuple3<String, String, String>(x._1(), x._2(), x._3()), x._4()));

        /*System.out.println("dopo la prima map"+ citiesPerYear2.collect().size());
        for(int i = 0; i < citiesPerYear.collect().size();i++){
            System.out.println("SONO QUIIIIIIIIIIIIIIIIIII nel while");

            System.out.println("TUPLAAAA nellA MAP" + citiesPerYear.collect().get(i));
        }*/

        JavaPairRDD<Tuple3<String,String,String>, Iterable<String>> hoursOfDay=citiesPerYear2.groupByKey();

        /*System.out.println("dopo la prima map"+ hoursOfDay.collect().size());
        for(int i = 0; i < hoursOfDay.collect().size();i++){
            System.out.println("SONO QUIIIIIIIIIIIIIIIIIII nel while");

            System.out.println("TUPLAAAA nellA MAP" + hoursOfDay.collect().get(i));
        }*/
        JavaPairRDD<Tuple3<String,String,String>,Iterable<String>> hoursOfDayClear=hoursOfDay.filter(new WheatherConditionExtractor());
        JavaPairRDD<Tuple3<String,String,String>,Integer> daysClear= hoursOfDayClear.mapToPair(x-> new Tuple2<>(x._1(),1));
        JavaPairRDD<Tuple3<String,String,String>,Integer> daysClearCounts= daysClear.reduceByKey((x,y)-> x+y);
        JavaPairRDD<Tuple2<String,String>,Tuple2<String, Integer>> monthsClearDaysForYearAndCity= daysClearCounts.mapToPair(x->new Tuple2<>(new Tuple2<>(x._1()._2(),x._1()._3()),new Tuple2<>(x._1()._1(),x._2())));
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>> clearMonthsForCityAndForYear= monthsClearDaysForYearAndCity.groupByKey();
        JavaPairRDD<Tuple2<String,String>, Iterable<Tuple2<String,Integer>>> clearCityPerYearWithMonths=clearMonthsForCityAndForYear.filter(new ClearCityExtractor());
        JavaPairRDD<String,String> clearCitiesPerYear= clearCityPerYearWithMonths.mapToPair(x->new Tuple2<>(x._1()._1(),x._1()._2()));
        JavaPairRDD<String,Iterable<String>> finalResult=clearCitiesPerYear.groupByKey();

        ArrayList<Tuple2<String,Iterable<String>>> output=Lists.newArrayList(finalResult.collect());

        System.out.println("SONO QUIIIIIIIIIIIIIIIIIII");
        while(output.iterator().hasNext()){
            System.out.println("SONO QUIIIIIIIIIIIIIIIIIII nel while");

            System.out.println("TUPLAAAA" + output.iterator().next());
        }



    }


    private static class WheatherConditionExtractor implements Function<Tuple2<Tuple3<String, String, String>, Iterable<String>>, Boolean> {


        @Override
        public Boolean call(Tuple2<Tuple3<String, String, String>, Iterable<String>> tuple3IterableTuple2) throws Exception {

            int count=0;

            while(tuple3IterableTuple2._2().iterator().hasNext()){

                if (tuple3IterableTuple2._2().iterator().next().equals("sky is clear"))
                    count++;

            }

            return (count>=4);


        }
    }

    private static class ClearCityExtractor implements Function<Tuple2<Tuple2<String, String>, Iterable<Tuple2<String, Integer>>>, Boolean> {
        @Override
        public Boolean call(Tuple2<Tuple2<String, String>, Iterable<Tuple2<String, Integer>>> tuple2IterableTuple2) throws Exception {

            Boolean isClear=true;

            while(tuple2IterableTuple2._2().iterator().hasNext()){
                if (tuple2IterableTuple2._2().iterator().next()._2()<1)
                    isClear=false;

            }


            return isClear;
        }
    }
}

