package com.icip.framework.topology.base;

import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.trident.state.RedisMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.DynamicBrokersReader;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.icip.framework.curator.ZooKeeperFactory;

public abstract class AbstractBaseTopo implements Serializable {

	protected static final Logger LOG = LoggerFactory
			.getLogger(AbstractBaseTopo.class);

	protected static final String STR = "str";

	private static final long serialVersionUID = -4848849855365547270L;

	private String masterPath = "/brokers";

	protected static int retry = 3;

	protected static final boolean flag = false;

	private String topic;
	private String spoutId;
	private String topologyName;
	private boolean ignoreZkOffsets = false;
	private long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();

	public AbstractBaseTopo(String topic, String spoutId, String topologyName,
			boolean ignoreZkOffsets, long startOffsetTime) {
		this.topic = topic;
		this.spoutId = spoutId;
		this.topologyName = topologyName;
		this.ignoreZkOffsets = ignoreZkOffsets;
		this.startOffsetTime = startOffsetTime;
	}

	public void checkTopic(String connectStr, String topic) throws Exception {
		Map<Object, Object> conf = new HashMap<Object, Object>();
		conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
		conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 3);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);

		GlobalPartitionInformation globalPartitionInformation = null;
		while (true) {
			try {
				DynamicBrokersReader dynamicBrokersReader = new DynamicBrokersReader(
						conf, connectStr, masterPath, topic);
				globalPartitionInformation = dynamicBrokersReader
						.getBrokerInfo();
			} catch (SocketTimeoutException e) {
				LOG.error("链接超时！", e);
				throw new SocketTimeoutException();
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("获取不到TOPIC信息，休眠10秒后重试!");
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
				}
			}
			if (globalPartitionInformation != null) {
				break;
			}
		}

	}

	public AbstractBaseTopo(String topic, String spoutId, String topologyName) {
		this(topic, spoutId, topologyName, false, kafka.api.OffsetRequest
				.EarliestTime());
	}

	public AbstractBaseTopo(String topic, String topologyName) {
		this(topic, topic, topologyName, false, kafka.api.OffsetRequest
				.EarliestTime());
	}

	public AbstractBaseTopo(String topologyName) {
		this(topologyName, topologyName, topologyName, false,
				kafka.api.OffsetRequest.EarliestTime());
	}

	public TransactionalTridentKafkaSpout getSpout() throws Exception {
		return getSpout(topic);
	}

	public TransactionalTridentKafkaSpout getSpout(String topic)
			throws Exception {
		String connectStr = ZooKeeperFactory.getConnectString(null);
		checkTopic(connectStr, topic);

		BrokerHosts brokerHosts = new ZkHosts(connectStr);
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts,
				topic, spoutId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// 开发从开始读，线上均是从offset中读取
		kafkaConfig.ignoreZkOffsets = this.ignoreZkOffsets;
		kafkaConfig.startOffsetTime = this.startOffsetTime;
		return new TransactionalTridentKafkaSpout(kafkaConfig);
	}

	public abstract StormTopology buildTopology(LocalDRPC drpc)
			throws Exception;

	public void execute(boolean flag) throws Exception {
		Config conf = new Config();

		conf.setNumWorkers(2);
		conf.setDebug(false);

		if (flag) {
			StormSubmitter.submitTopology(topologyName, conf,
					buildTopology(null));
		} else {
			// 远程调用
			LocalDRPC drpc = new LocalDRPC();

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, buildTopology(drpc));
			Thread.sleep(20000000);

			cluster.killTopology(topologyName);
			cluster.shutdown();
			System.exit(0);
		}
	}

	public void execute(boolean flag, LocalCluster cluster) throws Exception {
		if (cluster == null) {
			cluster = new LocalCluster();
		}
		Config conf = new Config();

		conf.setNumWorkers(5);
		conf.setDebug(false);

		if (flag) {
			StormSubmitter.submitTopology(topologyName, conf,
					buildTopology(null));
		} else {
			// 远程调用
			LocalDRPC drpc = new LocalDRPC();

			cluster.submitTopology(topologyName, conf, buildTopology(drpc));
			
		}
	}

	protected StateFactory getFactory(JedisPoolConfig poolConfig, String name,
			RedisDataTypeDescription.RedisDataType type) {
		RedisDataTypeDescription dataTypeDescriptionRt = new RedisDataTypeDescription(
				type, name);
		StateFactory factoryRt = RedisMapState.transactional(poolConfig,
				dataTypeDescriptionRt);
		return factoryRt;
	}

	protected StateFactory getFactory(JedisPoolConfig poolConfig, String name) {
		return getFactory(poolConfig, name,
				RedisDataTypeDescription.RedisDataType.HASH);
	}

	protected StateFactory getStrFactory(JedisPoolConfig poolConfig) {
		return getFactory(poolConfig, null,
				RedisDataTypeDescription.RedisDataType.STRING);
	}
}
