package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ParserCsvCity {

    public static ArrayList<CityInfo> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        String[] cityNames= null;
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

}
