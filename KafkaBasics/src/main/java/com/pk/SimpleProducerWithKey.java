package com.pk;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducerWithKey {
	
	public static void main(String[] args) {
		
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		//Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i=0;i<100;i++) {
			//create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","test", "Hi this is my first kafka producer  "+i);
			producer.send(record,(recordMetaData,exception)->{
				if(exception==null) {
					System.out.println("Received meta data \n"+
						"Topic : "+recordMetaData.topic()+"\n"+
						"Partition : "+recordMetaData.partition()+"\n"+
						"Offset : "+recordMetaData.offset()+"\n"+
						"Timestamp : "+recordMetaData.timestamp()+"\n"
						);
				}else {
					System.out.println("Error while producing records"+exception);
				}
			});
			
			//flush data
			producer.flush();
		}
		//flush and close the connection
		producer.close();
	}

}
