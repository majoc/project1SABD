package utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class City implements Serializable {

    private String city;
    private String date;
    private String weather_condition;

    public City() {
    }

    public City(String city, String date, String weather_condition) {
        this.city = city;
        this.date = date;
        this.weather_condition = weather_condition;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }



    public String getWeather_condition() {
        return weather_condition;
    }

    public void setWeather_condition(String weather_condition) {
        this.weather_condition = weather_condition;
    }



    public int getMonth () {

        //DateTimeFormatter dtf=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter format= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime date=DateTime.parse(this.getDate(), format);
        System.out.println("MESEEEE"+ date.getMonthOfYear());
        return date.getMonthOfYear();

    }

    public String getYear () {

        DateTime date=DateTime.parse(this.getDate());
        return ((Integer)date.getYear()).toString();
    }



    public void setDate(String date) {
        this.date = date;
    }

    public String getDate() {
        return date;
    }
}

