package com.pk;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		
		CountDownLatch latch = new CountDownLatch(1);
		System.out.println("creating consumer thread");
		Runnable runnable = new ConsumerDemoWithThread().new ConsumerThread(latch);
		
		Thread thread= new Thread(runnable);
		thread.start();
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			System.out.println("Shutdown Hook");
			((ConsumerThread) runnable).shutDown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("application has exited");
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}finally {
			System.out.println("application closing");
		}
		//subscribe consumer to our topics
		//pull for new data

		
		
	}
	
	private class ConsumerThread implements Runnable {
		
		private CountDownLatch latch;
		
		private KafkaConsumer<String, String> consumer;
		
		public ConsumerThread(CountDownLatch latch) {
			this.latch=latch;
			//create consumer config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-consumer-group");
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
			//create consumer
			consumer = new KafkaConsumer<>(properties);
			consumer.subscribe(Arrays.asList("first_topic"));
		}
		
		
		
		@Override
		public void run() {
			try {
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
			}catch(WakeupException e) {
				System.out.println("Recieved shutdown signal");
			}finally{
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutDown() {
			consumer.wakeup();
		}
		
	}
}
