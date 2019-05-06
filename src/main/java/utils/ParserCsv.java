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

                    for(int j=0; j< cityNames.length; ++j ){
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

            /*for(int z=0; z<cities.size();z++) {
                System.out.println(cities.get(z).getCity()+"  "+ cities.get(z).getDate()+"  "+ cities.get(z).getWeather_condition());
            }*/


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








        /*ArrayList<WeatherMeasurement> lineOfCities= new ArrayList<WeatherMeasurement>();

        ArrayList<String> CityNames= new ArrayList<String>();
        String[] csvValues = csvLine.split(",");

        //TODO codizione per scartare tuple
   *//*     if (csvValues.length != 7)
            return null;*//*

     int x=0;

     if (x==0){
         for(int j=1; j< csvValues.length; j++ ){
             CityNames.add(csvValues[j]);
         }


     }
*//*

        WeatherMeasurement city = new WeatherMeasurement(
                csvCity,
                csvValues[0], // timestamp
                csvValues[i] //weather condition

        );

        lineOfCities.add(city);*//*
         */

        return cities;
    }



}