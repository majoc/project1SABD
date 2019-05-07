package utils;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class CityInfo implements Serializable {

    private String cityName;
    private Double latitude;
    private Double longitude;
    private String nation;
    private String timezone;

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getNation() {

        return nation;
    }

    public String getTimezone() {

        return timezone;
    }

    public void setTimeZone() {

        String timezone = TimezoneMapper.latLngToTimezoneString(this.getLatitude(), this.getLongitude());
        this.timezone = timezone;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

  /*  public static void main(String[] args) {

        CityInfo city = new CityInfo();
        city.setLatitude(40.714272);
        city.setLongitude(-74.005966);

        city.setTimeZone();

        System.out.println("Nation" + " " + city.getTimezone() );

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            Date date = simpleDateFormat.parse("2012-10-01 17:00:00");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone(city.getTimezone()));

            System.out.println("ora"+ " " + simpleDateFormat.format(date));

        } catch (ParseException e) {
            e.printStackTrace();
        }


    }*/





}
