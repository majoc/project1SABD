package utils.Parser;

import entities.TemperatureMeasurement;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;

public class ParserCsvTemperature {
    public static ArrayList<TemperatureMeasurement> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line ;
        String cvsSplitBy = ",";
        String[] cityNames= null;
        ArrayList<TemperatureMeasurement> temperatureMeasurements = new ArrayList<>();


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
                        TemperatureMeasurement temperatureMeasurement = new TemperatureMeasurement();
                        temperatureMeasurement.setCity(cityNames[j]);
                        temperatureMeasurement.setDate(measurements[0]);
                        temperatureMeasurement.setTemperature(measurements[j+1]);

                        if(temperatureMeasurement.getTemperature().equals("") || temperatureMeasurement.getDate().equals(""))
                            temperatureMeasurement =null;

                        if (temperatureMeasurement!=null && !temperatureMeasurement.getTemperature().contains("."))
                            temperatureMeasurement.setTemperature(ParserCsvTemperature.fixBadValues(temperatureMeasurement.getTemperature()));



                        temperatureMeasurements.add(temperatureMeasurement);
                    }



                }
                index++;



            }


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

        return temperatureMeasurements;
    }

    private static String fixBadValues (String value){

        BigDecimal val= new BigDecimal(value).movePointLeft(value.length()-3);

        return val.toString();

    }

    public static void main(String[] args) {

        String file="data/prj1_dataset/weat_example.csv";

        ArrayList<TemperatureMeasurement> array = ParserCsvTemperature.parseCSV(file);

        for(int i=0; i<array.size();i++) {
            if(array.get(i).getCity().equals("Houston")) {
                System.out.println("Temp " + array.get(i).getTemperature());
            }
        }



    }





}
