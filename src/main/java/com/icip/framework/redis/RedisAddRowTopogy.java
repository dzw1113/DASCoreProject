package com.icip.framework.redis;

import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateUpdater;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 账单主表插入demo,本地模式
 * 
 * @author lenovo
 * 
 */
public class RedisAddRowTopogy {

	static Fields K_V = new Fields("bill_key", "bill_value");
	@SuppressWarnings("unchecked")
	static FixedBatchSpout spout = new FixedBatchSpout(K_V, 11, new Values(
			"BILL_NO", "BS2015122400035530"),
			new Values("CHARGE_NO", "C003002"), new Values("PAYMENT_DATE",
					"20151229231449"), new Values("UNIT_PRICE", "0.1"),
			new Values("PAYMENT_AMOUNT", "3.40"), new Values("LAST_READING",
					"3"), new Values("THIS_READING", "20"), new Values(
					"USE_NUMBER", "17"), new Values("RECEIVED_AMOUNT", "3.40"),
			new Values("MOUNT", "0.1"), new Values("CHARGE_MARK", "备注1"));

	@SuppressWarnings("unchecked")
	static FixedBatchSpout spout2 = new FixedBatchSpout(K_V, 4, new Values(
			"BILL_NO", "BS2015122400035524"), new Values("BILL_DATE",
			"20151124"), new Values("ROOT_NO", "B区2幢1"), new Values(
			"OWNER_NAME", "赵二"));

	public static StormTopology buildTopology(String redisHost,
			Integer redisPort, String password) {

		spout2.setCycle(false);

		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
				.setHost(redisHost).setPort(redisPort).setPassword(password)
				.build();

		RedisStoreMapper storeMapper = new ColStoreMapper(
				"bs_bill_details:BS2015122400035524");
		RedisState.Factory factory = new RedisState.Factory(poolConfig);

		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("spout2", spout2);
		// 加工过程.each(new RedisMapperFunction(redisHost, redisPort,
		// password),K_V);

		stream.partitionPersist(factory, K_V,
				new RedisStateUpdater(storeMapper).withExpire(86400000),
				new Fields());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {

		String redisHost = "120.25.122.213";
		Integer redisPort = 6379;
		String password = "admin";

		Config conf = new Config();
		conf.setMaxSpoutPending(5);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test_wordCounter_for_redis", conf,
				buildTopology(redisHost, redisPort, password));
		Thread.sleep(60 * 1000);
		cluster.killTopology("test_wordCounter_for_redis");
		cluster.shutdown();
		System.exit(0);
	}
}
