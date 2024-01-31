package com.maveric.streambasics;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

// Serde Serializer-Deserializer
public class ViolationCountGenerator {
    private static final Logger Log = LoggerFactory.getLogger(ViolationCountGenerator.class);

    public static void main(String[] args) throws Exception {

    	Properties props = KafkaPropertiesReader.read("application.properties");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "challan-consumer-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		System.out.println("After Config");
		// Create Kafka consumer
		Consumer<String, String> consumer = new KafkaConsumer<>(props);
		// Subscribe to the vehicleOverSpeed topic
		consumer.subscribe(Collections.singletonList("vehicleOverSpeedTopic"));

        Log.info("****properties" + props);
        StreamsBuilder builder = new StreamsBuilder();
        //source processor giving us stream from topic input-words
        KStream<String, String> inputStream = builder.stream("vehicleOverSpeedTopic", Consumed.with(Serdes.String(), Serdes.String()));
        
        KGroupedStream<String,String>groupedStream=inputStream.groupByKey();
        KTable<String,Long>countTable=groupedStream.count();
        KStream<String,Long>outStream=countTable.toStream();
        outStream.peek((key,value)-> System.out.println("outstream peek, key="+key+" count="+value));
//                .to("messages-key-count");
        
        outStream.filter((key, value) -> value >= 3)
        	.foreach((key, value) -> {
//        		callNewMethod(key, value);
        		System.out.println("Called Test Method");
            } );
        
        Topology topology = builder.build();
//        System.out.println("topology starts");
        Log.info("*****topology=" + topology.describe());
//        System.out.println("topology ends");
        try (KafkaStreams streams = new KafkaStreams(topology, props)) {
            streams.start();
            Thread.sleep(100000000);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("*** application closed");
        }));
    }
    
    

}
