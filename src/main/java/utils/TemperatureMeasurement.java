package utils;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public class TemperatureMeasurement implements Serializable {


    private String city;
    private String date;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    private String temperature;

    public String getMonth() {

        //DateTimeFormatter dtf=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime date = LocalDateTime.parse(this.getDate(), format);
        return ((Integer) date.getMonthOfYear()).toString();


    }

    public String getYear() {

        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime date = LocalDateTime.parse(this.getDate(), format);
        return ((Integer) date.getYear()).toString();

    }

    public String getDay() {

        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime date = LocalDateTime.parse(this.getDate(), format);
        return ((Integer) date.getDayOfMonth()).toString();

    }


    public void setDate(String date) {
        this.date = date;
    }

    public String getDate() {


        return date;
    }


}
