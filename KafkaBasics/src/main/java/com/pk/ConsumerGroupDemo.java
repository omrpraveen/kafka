package com.pk;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerGroupDemo {

	public static void main(String[] args) {
		
		//create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-consumer-group");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		//subscribe consumer to our topics
		consumer.subscribe(Arrays.asList("first_topic"));
		//pull for new data

		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			records.forEach((record)->{
				System.out.println("Topic " + record.topic()+"\n");
				System.out.println("Partition " + record.partition()+"\n");
				System.out.println("Offset " + record.offset()+"\n");
				System.out.println("Key " + record.key()+"\n");
				System.out.println("Value " + record.value());
			});
		}
		
	}
}
