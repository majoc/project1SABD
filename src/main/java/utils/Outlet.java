package utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class Outlet implements Serializable {

    private String id;
    private String timestamp;
    private String value;
    private String property;
    private String plug_id;
    private String household_id;
    private String house_id;

    public Outlet(String id, String timestamp, String value, String property, String plug_id, String household_id, String house_id) {
        this.id = id;
        this.timestamp = timestamp;
        this.value = value;
        this.property = property;
        this.plug_id = plug_id;
        this.household_id = household_id;
        this.house_id = house_id;
    }



    public String getId() {
        return id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value;
    }

    public String getPlug_id() {
        return plug_id;
    }

    public String getHousehold_id() {
        return household_id;
    }

    public String getHouse_id() {
        return house_id;
    }

    public String getProperty() {
        return property;
    }


    public String getDate() {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getDefault());
        calendar.setTimeInMillis(Long.parseLong(timestamp) * 1000);

        // Only for test
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = sdf.format(calendar.getTime());
        System.out.println(dateString);


        return dateString;
    }


    public String getMonth () {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getDefault());
        calendar.setTimeInMillis(Long.parseLong(timestamp) * 1000);

        return String.valueOf(calendar.get(Calendar.MONTH));

    }


    @Override
    public String toString(){
        return getId() + ", " + getTimestamp() + ", " + getValue() + ", " + getPlug_id() + ", " + getHousehold_id() +
                " " + getHouse_id();
    }

}


