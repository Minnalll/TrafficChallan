package com.vehicleoverspeed.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.vehicleoverspeed.model.VehicleOverSpeed;
import com.vehicleoverspeed.service.VehicleOverSpeedProducerService;

@RestController
public class VehicleOverSpeedController {
	
    @Autowired
    private VehicleOverSpeedProducerService vehicleOverSpeedProducerService;

    @PostMapping("/publish")
    public String publishMessage(@RequestBody VehicleOverSpeed vehicleOverSpeed)
    {
    	vehicleOverSpeedProducerService.sendMessage(vehicleOverSpeed);
        System.out.println("Successfully Published the Vehicle Over Speeding Details = '" + vehicleOverSpeed + "' to the vehicleOverSpeedTopic");
        return "Successfully Published the Vehicle Over Speeding Details = '" + vehicleOverSpeed + "' to the vehicleOverSpeedTopic";
    }

}
