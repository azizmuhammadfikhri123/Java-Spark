package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
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
}
