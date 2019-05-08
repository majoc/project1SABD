package entities;

import eu.bitm.NominatimReverseGeocoding.NominatimReverseGeocodingJAPI;
import utils.TimezoneMapper;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
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



}
