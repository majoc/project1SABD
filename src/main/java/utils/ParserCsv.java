package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ParserCsv {

    public ParserCsv() {
    }

    public static ArrayList <WeatherMeasurement> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        String[] cityNames= null;
        ArrayList<WeatherMeasurement> cities = new ArrayList<>();


        try {

            br = new BufferedReader(new FileReader(csvFile));
            int index=0;
            while ((line = br.readLine()) != null) {

                if(index==0) {
                    // use comma as separator
                    String[] firstLine = line.split(cvsSplitBy,-1);

                    cityNames= Arrays.copyOfRange(firstLine, 1, firstLine.length);

                }
                else {

                    String[] measurements = line.split(cvsSplitBy,-1);


                    for(int j=0; j< cityNames.length; j++ ){
                        WeatherMeasurement weatherMeasurement = new WeatherMeasurement();
                        weatherMeasurement.setCity(cityNames[j]);
                        weatherMeasurement.setDate(measurements[0]);
                        weatherMeasurement.setWeather_condition(measurements[j+1]);

                        if(weatherMeasurement.getWeather_condition().equals("") || weatherMeasurement.getDate().equals(""))
                            weatherMeasurement =null;


                        cities.add(weatherMeasurement);
                    }



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