package utils.Parser;

import entities.CityInfo;
import eu.bitm.NominatimReverseGeocoding.NominatimReverseGeocodingJAPI;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


import java.util.Locale;

public class BuilderCleanerCityRDD {

    public static Tuple2<JavaRDD<CityInfo>,Long> construct_cleanRDD(JavaSparkContext sc, String pathToFileCities){

        Long preprocessingTime=System.currentTimeMillis();

        JavaRDD<String> initialcity= sc.textFile(pathToFileCities);
        String header=initialcity.first();
        JavaRDD<String> initialCityCleaned = initialcity.filter(x->!x.equals(header));


        //creating city rdd for query2, filled up with nation info
        JavaRDD<CityInfo> cityRDD= initialCityCleaned.map((Function<String, CityInfo>)
                s -> BuilderCleanerCityRDD.parseLine(s,"query2")).filter(x-> x!=null);

        return new Tuple2<>(cityRDD,System.currentTimeMillis()-preprocessingTime);


    }

    static CityInfo parseLine(String line, String id){

        String cvsSplitBy = ",";

        String[] cityInfo = line.split(cvsSplitBy,-1);



        CityInfo city = new CityInfo();
        city.setCityName(cityInfo[0]);
        city.setLatitude(Double.parseDouble(cityInfo[1]));
        city.setLongitude(Double.parseDouble(cityInfo[2]));

        city.setTimeZone();

        if(city.getCityName().equals("") || (city.getLatitude() > 90.0 && city.getLatitude() < -90.0) || (city.getLongitude() > 180.0 && city.getLatitude() < -180.0) ) {
            city = null;
        }

        if(city != null)

            city.setNation(BuilderCleanerCityRDD.getNationfromCoordinates(city));


        return city;
    }





    private static String getNationfromCoordinates(CityInfo city) {

        Locale langEnglish = new Locale.Builder().setLanguage("en").build();

        NominatimReverseGeocodingJAPI nominatim1 = new NominatimReverseGeocodingJAPI();
        String country = nominatim1.getAdress(city.getLatitude(), city.getLongitude()).getCountryCode();
        Locale countryEnglish = new Locale.Builder().setRegion(country).build();

        return countryEnglish.getDisplayCountry(langEnglish);


    }

}
