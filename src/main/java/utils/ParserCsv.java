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

    public static ArrayList <City> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        String[] cityNames= null;
        ArrayList<City> cities = new ArrayList<>();


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
                        City city = new City();
                        city.setCity(cityNames[j]);
                        city.setDate(measurements[0]);
                        city.setWeather_condition(measurements[j+1]);

                        if(city.getWeather_condition().equals("") || city.getDate().equals(""))
                            city=null;


                        cities.add(city);
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








        /*ArrayList<City> lineOfCities= new ArrayList<City>();

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

        City city = new City(
                csvCity,
                csvValues[0], // timestamp
                csvValues[i] //weather condition

        );

        lineOfCities.add(city);*//*
         */

        return cities;
    }



}