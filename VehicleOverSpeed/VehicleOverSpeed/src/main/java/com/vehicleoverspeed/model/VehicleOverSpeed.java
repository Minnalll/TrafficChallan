package com.vehicleoverspeed.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class VehicleOverSpeed {

    private String vehicleNumber;
    private double speed;
    private String email;
}
