package queries;



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
import utils.ConvertDatetime;
import utils.Parser.ParserCSVHeader;
import utils.Parser.ParserCsvCity;
import utils.Parser.ParserCsvTemperature;
import utils.SaveOutput;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;


public class Query3 {

    private static String pathToHDFS= "hdfs://172.19.0.5:54310/output";

    private static String pathToFileTemperature = "data/prj1_dataset/temperature.csv";
    private static String pathToFileCities = "data/prj1_dataset/city_attributes.csv";


    private static String year1 = "2016";
    private static String year2 = "2017";


    public static void query3() {

        //System.setProperty("hadoop.home.dir","C:\\winutils");

        SparkSession sparkSession= SparkSession.builder()
                .master("local[*]")
                .appName("Weather Analyzer")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        sc.setLogLevel("ERROR");

        top3cityMaxDifference(sc, sparkSession);

        sc.stop();


    }


    public static void top3cityMaxDifference( JavaSparkContext sc, SparkSession sparkSession) {

        //creating cityRDD
        JavaRDD<String> initialcity= sc.textFile(pathToFileCities/*"hdfs://localhost:54310/data/city_attributes.csv.COMPLETED"*/);
        String header=initialcity.first();
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));

        JavaRDD<CityInfo> cityRDD= initialCityCleaned.map((Function<String, CityInfo>) s -> ParserCsvCity.parseLine(s,"query3"));


        //creating temperature RDD
        JavaRDD<String> initialtemperature= sc.textFile(pathToFileTemperature/*"hdfs://localhost:54310/data/temperature.csv"*/);
        String headerCityList=initialtemperature.first();
        String[] cityList = ParserCSVHeader.getListCities(headerCityList);
        JavaRDD<String> initialTemperatureCleaned = initialtemperature.filter(x->!x.equals(headerCityList));



        JavaRDD<TemperatureMeasurement> temperatureInitial =initialTemperatureCleaned.flatMap((FlatMapFunction<String, TemperatureMeasurement>) s -> {
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


        // constructing RDD for subsequent join
        JavaPairRDD<String,TemperatureMeasurement> cityTemperatures = temperatureRDDclean.mapToPair(x -> new Tuple2<>(x.getCity(),x));
        JavaPairRDD<String,CityInfo> cityCityInfo = cityRDD.mapToPair(x -> new Tuple2<>(x.getCityName(),x));

        JavaPairRDD<String,Tuple2<TemperatureMeasurement,CityInfo>> convertedJoin  = cityTemperatures
                //performing join operation
                .join(cityCityInfo)

                //mapping previous RDD into a new one with with converted DateTime
                .mapToPair(x->new Tuple2<>(x._1(),
                new Tuple2<>(new TemperatureMeasurement(x._2()._1().getCity(),
                        ConvertDatetime.convert(x._2()._2().getTimezone(),
                        x._2()._1().getDate()),
                        x._2()._1().getTemperature()),
                        x._2()._2()))).cache();

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureFirstGroup = temperatureMean(convertedJoin,"6","7","8","9");

        JavaPairRDD<Tuple3<String,String,String>,Double> temperatureSecondGroup = temperatureMean(convertedJoin,"1","2","3","4");

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
                    t -> new Tuple2<>(t._1(), Iterators.partition(t._2().iterator(),3).next()))

                //mapping for easier index manipulation
                .mapToPair(x-> new Tuple2<>(x._1(),Lists.newArrayList(x._2().iterator()))).cache();


        //top chart of all cities in 2016
        JavaPairRDD<Tuple2<String,String>, ArrayList<Tuple2<String,Double>>> intermediateFirst=temperatureDiffGrouped

                //separating the subset relative to year1--> it's still sorted
                .filter(x->x._1()._2().equals(year1))

                //this mapping is used to convert the sorted iterable list, relative to year1,
                //into an array list in order to make easier subsequent index manipulation
                .mapToPair(x-> new Tuple2<>(x._1(),Lists.newArrayList(x._2().iterator()))).cache();



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


        //SaveOutput s=new SaveOutput();
        //s.saveOutputQuery3(finalRDD,sparkSession,pathToHDFS);

        for (int i =0; i< finalRDD.collect().size();i++){
                System.out.println(finalRDD.collect().get(i));

        }


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

        //return an RDD in which key is the triple (nation, year,city) and the value is the temperature mean
        return temperatureByKey.mapToPair(x->new Tuple2<>(x._1(),x._2()._1()/x._2()._2()));



    }



}
