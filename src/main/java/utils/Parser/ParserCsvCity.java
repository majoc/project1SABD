package utils.Parser;

import entities.CityInfo;
import eu.bitm.NominatimReverseGeocoding.NominatimReverseGeocodingJAPI;


import java.util.Locale;

public class ParserCsvCity {

    public static CityInfo parseLine(String line, String id){

        String cvsSplitBy = ",";

        String[] cityInfo = line.split(cvsSplitBy,-1);



        CityInfo city = new CityInfo();
        city.setCityName(cityInfo[0]);
        city.setLatitude(Double.parseDouble(cityInfo[1]));
        city.setLongitude(Double.parseDouble(cityInfo[2]));

        city.setTimeZone();

        if(id.equals("query2") || id.equals("query3")){
            city.setNation(ParserCsvCity.getNationfromCoordinates(city));
        }

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
