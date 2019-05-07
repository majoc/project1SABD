package utils;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WeatherMeasurement implements Serializable {

    private String city;
    private String date;
    private String weather_condition;

    public WeatherMeasurement() {
    }

    public WeatherMeasurement(String city, String date, String weather_condition) {
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

