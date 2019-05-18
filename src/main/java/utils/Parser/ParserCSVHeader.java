package utils.Parser;

import java.util.Arrays;

public class ParserCSVHeader {

    public static String[] getListCities(String firstline){

        String cvsSplitBy = ",";

        String[] cityInfo = firstline.split(cvsSplitBy,-1);

        return Arrays.copyOfRange(cityInfo,1,cityInfo.length);


    }
}
