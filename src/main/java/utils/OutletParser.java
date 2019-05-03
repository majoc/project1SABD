package utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;



    public class OutletParser {

        public static Outlet parseJson(String jsonLine) {

            System.out.println(jsonLine);

            com.fasterxml.jackson.databind.ObjectMapper objectMapper = new ObjectMapper();
            Outlet outlet = null;

            try {
                outlet = objectMapper.readValue(jsonLine, Outlet.class);
            } catch (IOException e) {
                //e.printStackTrace();
            }
            return outlet;
        }

        public static Outlet parseCSV(String csvLine) {

            Outlet outlet = null;
            String[] csvValues = csvLine.split(",");

            if (csvValues.length != 7)
                return null;

//            1464894,1377987280,3.216,0,1,0,3

            outlet = new Outlet(
                    csvValues[0], // id
                    csvValues[1], // timestamp
                    csvValues[2], // value
                    csvValues[3], // property
                    csvValues[4], // plug_id
                    csvValues[5], // household_id
                    csvValues[6]  // house_id
            );

            return outlet;
        }



    }




