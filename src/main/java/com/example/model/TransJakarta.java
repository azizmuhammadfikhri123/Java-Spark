package com.example.model;

public class TransJakarta {
  private String id;
  private Integer koridor;
  private String current_trip_id;
  private String color;
  private String gps_datetime;
  private Double dtd;
  private String trip_name;
  private Integer course;
  private String location;
  private String bus_code;
  private Double speed;
  private String timestamp;
  private Double latitude;
  private Double longitude;

  public TransJakarta(String id, Integer koridor, String current_trip_id, String color, String gps_datetime, Double dtd, String trip_name, Integer course, String location, String bus_code, Double speed, String timestamp, Double latitude, Double longitude) {
    this.id = id;
    this.koridor = koridor;
    this.current_trip_id = current_trip_id;
    this.color = color;
    this.gps_datetime = gps_datetime;
    this.dtd = dtd;
    this.trip_name = trip_name;
    this.course = course;
    this.location = location;
    this.bus_code = bus_code;
    this.speed = speed;
    this.timestamp = timestamp;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Integer getKoridor() {
    return koridor;
  }

  public void setKoridor(Integer koridor) {
    this.koridor = koridor;
  }

  public String getCurrent_trip_id() {
    return current_trip_id;
  }

  public void setCurrent_trip_id(String current_trip_id) {
    this.current_trip_id = current_trip_id;
  }

  public String getColor() {
    return color;
  }

  public void setColor(String color) {
    this.color = color;
  }

  public String getGps_datetime() {
    return gps_datetime;
  }

  public void setGps_datetime(String gps_datetime) {
    this.gps_datetime = gps_datetime;
  }

  public Double getDtd() {
    return dtd;
  }

  public void setDtd(Double dtd) {
    this.dtd = dtd;
  }

  public String getTrip_name() {
    return trip_name;
  }

  public void setTrip_name(String trip_name) {
    this.trip_name = trip_name;
  }

  public Integer getCourse() {
    return course;
  }

  public void setCourse(Integer course) {
    this.course = course;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getBus_code() {
    return bus_code;
  }

  public void setBus_code(String bus_code) {
    this.bus_code = bus_code;
  }

  public Double getSpeed() {
    return speed;
  }

  public void setSpeed(Double speed) {
    this.speed = speed;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
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



}
