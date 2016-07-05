package com.icip.framework.curator;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.icip.das.util.PropertiesUtil;

/**
 * 
 * @Description: zk工厂
 * @author
 * @date 2016年3月21日 下午4:01:47
 * @update
 */
public class ZooKeeperFactory {

	private static final String ZOOKEEPER_PROPERTIES = "/zookeeper.properties";

	private static PropertiesUtil pu = PropertiesUtil
			.getInstance(ZOOKEEPER_PROPERTIES);

	private static final String CONNECT_STRING = "connect_string";

	private static final int MAX_RETRIES = 3;

	private static final int BASE_SLEEP_TIMEMS = 3000;

	public static CuratorFramework get() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(
				BASE_SLEEP_TIMEMS, MAX_RETRIES);
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(pu.getValue(CONNECT_STRING))
				.retryPolicy(retryPolicy).build();
		client.start();
		return client;
	}

	public static String getConnectString(String connect_string) {
		return StringUtils.isEmpty(connect_string) ? pu
				.getValue(CONNECT_STRING) : connect_string;
	}
}
