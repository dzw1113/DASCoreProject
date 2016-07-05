package com.icip.das.core.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * @Description: 生成Producer
 * @author  
 * @date 2016年2月29日 下午2:27:52 
 * @update
 */
public class KafkaProducerFactory {

	private static org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;
	
	private static Object lock = new Object();
	
	private static final Properties props = KafkaConfigPropertiesLoader.PRO_INSTANCE.getProperties();
	
	public static KafkaProducer<Integer, String> getProducer(){
		if(null == producer){
			synchronized(lock){
				if(null == producer){
					producer = new org.apache.kafka.clients.producer.KafkaProducer<Integer, String>(props);
				}
			}
		}
		return producer;
	}
	
}
