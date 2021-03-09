package com.pk;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemoAssignSeek {
	
	/**
	 * assign and seek are mostly used to reply data or fetch a specific message 
	 * 
	 */

	public static void main(String[] args) {
		
		//create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		
		//assign and seek 
		TopicPartition partitionReadFrom = new TopicPartition("first_topic",0);
		
		//assign the partition to read
		consumer.assign(Arrays.asList(partitionReadFrom));
		
		//assign the offset to read 
		long offSetToReadFrom = 15L;
		consumer.seek(partitionReadFrom, offSetToReadFrom);
		
		int numberOfMessagesToRead = 5;
		boolean keepOnLoading=true;
		int numberOfMessagesReadSoFar = 0;
		
		while(keepOnLoading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Topic " + record.topic()+"\n");
				System.out.println("Partition " + record.partition()+"\n");
				System.out.println("Offset " + record.offset()+"\n");
				System.out.println("Key " + record.key()+"\n");
				System.out.println("Value " + record.value());
				numberOfMessagesReadSoFar += 1;
				if(numberOfMessagesToRead <= numberOfMessagesReadSoFar) {
					keepOnLoading = false;
					break;
				}
			}
			
		}
		
	}
}
