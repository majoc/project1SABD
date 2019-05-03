package utils;

import java.io.Serializable;

/**
 * author: Torlone (Roma Tre)
 */
public class Tweet implements Serializable {

    private long id;
    private String user;
    private String userName;
    private String text;
    private String place;
    private String country;
    private String lang;

    public String getUserName() {
        return userName;
    }

    public String getLang() {
        return lang;
    }

    public long getId() {
        return id;
    }

    public String getUser() { return user;}

    public String getText() {
        return text;
    }

    public String getPlace() {
        return place;
    }

    public String getCountry() {
        return country;
    }


    @Override
    public String toString(){
        return getId() + ", " + getUser() + ", " + getText() + ", " + getPlace() + ", " + getCountry();
    }
}