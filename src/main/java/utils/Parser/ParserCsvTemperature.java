package utils.Parser;

import java.math.BigDecimal;

public class ParserCsvTemperature {



    public static String fixBadValues (String value){

        BigDecimal val= new BigDecimal(value).movePointLeft(value.length()-3);

        return val.toString();

    }


}
