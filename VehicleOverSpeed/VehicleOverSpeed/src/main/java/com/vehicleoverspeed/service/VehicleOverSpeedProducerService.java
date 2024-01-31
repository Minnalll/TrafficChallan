package com.vehicleoverspeed.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.vehicleoverspeed.model.VehicleOverSpeed;

@Service
public class VehicleOverSpeedProducerService {

	@Autowired
    private KafkaTemplate<String, VehicleOverSpeed> kafkaTemplate;

	private final static String TOPIC_NAME = "vehicleOverSpeedTopic";
    
	

    public void sendMessage(VehicleOverSpeed vehicleOverSpeed) {
        kafkaTemplate.send(TOPIC_NAME, vehicleOverSpeed.getVehicleNumber(), vehicleOverSpeed);
    }

}
