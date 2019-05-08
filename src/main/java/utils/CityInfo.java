package utils;

import eu.bitm.NominatimReverseGeocoding.NominatimReverseGeocodingJAPI;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
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

    public static void main(String[] args) {

        Double latitude = 33.749001;
        Double longitude = -84.387978;

       /*Locale langEnglish  = new Locale.Builder().setLanguage("en").build();

        NominatimReverseGeocodingJAPI nominatim1 = new NominatimReverseGeocodingJAPI();
        String country = nominatim1.getAdress(latitude, longitude).getCountryCode();
        Locale countryEnglish = new Locale.Builder().setRegion(country).build();

        String nation = countryEnglish.getDisplayCountry(langEnglish);


        System.out.println("Country " + nation);*/

        String x= "123456";


        BigDecimal val= new BigDecimal(x).movePointLeft(x.length()-2);


        System.out.println("Valore " + val.toString());


    }





}
