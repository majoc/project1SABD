package utils.Parser;

import entities.HumidityMeasurement;
import entities.PressureMeasurement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ParserCsvPressure {

    public ParserCsvPressure() {
    }

    public static ArrayList<PressureMeasurement> parseCSV(String csvFile) {

        BufferedReader br = null;
        String line;
        String cvsSplitBy = ",";
        String[] cityNames= null;
        ArrayList<PressureMeasurement> pressureMeasurements = new ArrayList<>();


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
                        PressureMeasurement pressureMeasurement = new PressureMeasurement();
                        pressureMeasurement.setCity(cityNames[j]);
                        pressureMeasurement.setDate(measurements[0]);
                        pressureMeasurement.setPressure(measurements[j+1]);

                        if(pressureMeasurement.getPressure().equals("") || pressureMeasurement.getDate().equals(""))
                            pressureMeasurement =null;

                        pressureMeasurements.add(pressureMeasurement);
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

        return pressureMeasurements;
    }


}
