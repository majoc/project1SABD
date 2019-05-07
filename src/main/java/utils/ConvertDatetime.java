package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ConvertDatetime {

    public static String convert(String timezone, String date) {

        Date dateTime = new Date();



        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            dateTime = simpleDateFormat.parse(date);
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(timezone));


        } catch (ParseException e) {
            e.printStackTrace();
        }

        return simpleDateFormat.format(dateTime);
    }

}
