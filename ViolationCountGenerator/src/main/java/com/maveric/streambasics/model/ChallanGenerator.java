package com.maveric.streambasics.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.maveric.streambasics.enums.Category;
import com.maveric.streambasics.enums.VehicleType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@Setter
@ToString
public class ChallanGenerator {

	private Long id;

	private String vehicleNumber;

	private String city;

	private BigDecimal amount;

	private VehicleType vehicleType; 

	private Category category; 

	private LocalDateTime created;
}
