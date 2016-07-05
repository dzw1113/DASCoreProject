package com.icip.das.core.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.framework.constans.DASConstants;
import com.icip.framework.curator.CuratorTools;

public class KafkaConfigPropertiesLoader {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConfigPropertiesLoader.class);
	
	private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	private static final String CLIENT_ID = "client.id";
	private static final String RETRIES = "retries";
	private static final String KEY_SERIALIZER = "key.serializer";
	private static final String VALUE_SERIALIZER = "value.serializer";
	private static final String ACKS = "acks";
	private static final String SYSTEM = "system";
	private static final String KAFKA_PROPERTIES = "kafka.properties";

	public static final KafkaConfigPropertiesLoader PRO_INSTANCE = new KafkaConfigPropertiesLoader();
	
	private Properties pro;

	public Properties getProperties(){
		if(null == pro){
			synchronized(this){
				if(null == pro){
					try{
						Properties props = CuratorTools.downCfgProperties(DASConstants.KAFKA_PATH);
						this.pro = loadPropValue(props);
					}catch(Exception e){
						
					}
				}
				if(null == pro){
					Properties props = loadProperties();
					this.pro = loadPropValue(props);
				}
			}
		}
		return pro;
	}

	private Properties loadProperties(){
		Properties props = new Properties();
		FileInputStream in = null;
		try{
			File propertiesFile = new File(this.getClass().getResource("/").getPath() + KafkaConfigPropertiesLoader.KAFKA_PROPERTIES);
			if(null == propertiesFile || !propertiesFile.exists())
				throw new FileNotFoundException();
			
			in = new FileInputStream(propertiesFile);
			props.load(in);
		}catch(FileNotFoundException e){
			logger.error(e.toString(), e);
		}catch(IOException ex){
			logger.error(ex.toString(), ex);
		}finally{
			try{
				in.close();
			}catch(Exception e){
				logger.error(e.toString(), e);
			}
		}
		return props;
	}
	
	private Properties loadPropValue(Properties props) {
		props.put(KafkaConfigPropertiesLoader.BOOTSTRAP_SERVERS,getValue(props,KafkaConfigPropertiesLoader.BOOTSTRAP_SERVERS));
        props.put(KafkaConfigPropertiesLoader.CLIENT_ID,getValue(props,KafkaConfigPropertiesLoader.CLIENT_ID));
        props.put(KafkaConfigPropertiesLoader.KEY_SERIALIZER, getValue(props,KafkaConfigPropertiesLoader.KEY_SERIALIZER));
        props.put(KafkaConfigPropertiesLoader.VALUE_SERIALIZER, getValue(props,KafkaConfigPropertiesLoader.VALUE_SERIALIZER));
        props.put(KafkaConfigPropertiesLoader.ACKS, getValue(props,KafkaConfigPropertiesLoader.ACKS));
        props.put(KafkaConfigPropertiesLoader.RETRIES, getValue(props,KafkaConfigPropertiesLoader.RETRIES));
        props.put(KafkaConfigPropertiesLoader.CLIENT_ID, getValue(props,KafkaConfigPropertiesLoader.CLIENT_ID));
        props.put(KafkaConfigPropertiesLoader.SYSTEM, getValue(props,KafkaConfigPropertiesLoader.SYSTEM));
		return props;
	}
	
	private String getValue(Properties props,String key) {
		if (props.containsKey(key)) {
			String value = props.getProperty(key);
			return value;
		} else
			return "";
	}
	
}

