package entities;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public class PressureMeasurement implements Serializable {

    private String city;
    private String date;
    private String pressure;

    public PressureMeasurement() {
    }

    public PressureMeasurement(String city, String date, String pressure) {
        this.city = city;
        this.date = date;
        this.pressure = pressure;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPressure() {
        return pressure;
    }

    public void setPressure(String pressure) {
        this.pressure = pressure;
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
}
