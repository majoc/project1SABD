package utils.Parser;

import entities.HumidityMeasurement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ParserCsvHumidity {

    public ParserCsvHumidity() {
    }

    public static ArrayList<HumidityMeasurement> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line;
        String cvsSplitBy = ",";
        String[] cityNames= null;
        ArrayList<HumidityMeasurement> humidityMeasurements = new ArrayList<>();


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
                        HumidityMeasurement humidityMeasurement = new HumidityMeasurement();
                        humidityMeasurement.setCity(cityNames[j]);
                        humidityMeasurement.setDate(measurements[0]);
                        humidityMeasurement.setHumidity(measurements[j+1]);

                        if(humidityMeasurement.getHumidity().equals("") || humidityMeasurement.getDate().equals(""))
                            humidityMeasurement =null;

                        humidityMeasurements.add(humidityMeasurement);
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

        return humidityMeasurements;
    }
}
