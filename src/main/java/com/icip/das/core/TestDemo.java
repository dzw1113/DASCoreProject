package com.icip.das.core;

import java.sql.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.jdbc.DataSourceUtil;
import com.icip.das.core.kafka.KafkaProducerFactory;
import com.icip.das.core.redis.RedisClientPool;

public class TestDemo {
	
	public static BlockingQueue<String> queue = new LinkedBlockingQueue<String>(3);
	
	public static void main(String[] args) {
		// 初始化kafka
		KafkaProducerFactory.getProducer();
		// 初始化数据源配置
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		DataInitLoader.getTableConfig();
		RedisClientPool.getInstance();

		Connection conn = DataSourceUtil.getConnection("DAS");
		System.err.println(conn);
	}
}

class MyThread extends Thread {
	@Override
	public void run() {
	}
}