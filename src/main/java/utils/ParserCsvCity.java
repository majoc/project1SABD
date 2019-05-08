package utils;

import eu.bitm.NominatimReverseGeocoding.NominatimReverseGeocodingJAPI;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

public class ParserCsvCity {

    public static ArrayList<CityInfo> parseCSV(String csvFile,String id) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        ArrayList<CityInfo> cities = new ArrayList<>();


        try {

            br = new BufferedReader(new FileReader(csvFile));
            int index=0;
            while ((line = br.readLine()) != null) {

                if(index !=0) {


                    String[] cityInfo = line.split(cvsSplitBy,-1);



                    CityInfo city = new CityInfo();
                    city.setCityName(cityInfo[0]);
                    city.setLatitude(Double.parseDouble(cityInfo[1]));
                    city.setLongitude(Double.parseDouble(cityInfo[2]));

                    city.setTimeZone();

                    if(id.equals("query2")){
                        city.setNation(ParserCsvCity.getNationfromCoordinates(city));
                    }

                    cities.add(city);

                }
                index++;



            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return cities;
    }

    private static String getNationfromCoordinates(CityInfo city) {

        Locale langEnglish = new Locale.Builder().setLanguage("en").build();

        NominatimReverseGeocodingJAPI nominatim1 = new NominatimReverseGeocodingJAPI();
        String country = nominatim1.getAdress(city.getLatitude(), city.getLongitude()).getCountryCode();
        Locale countryEnglish = new Locale.Builder().setRegion(country).build();

        return countryEnglish.getDisplayCountry(langEnglish);


    }

}
