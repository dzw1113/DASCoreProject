package com.icip.framework.topology;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.SaveHouseInfoByTransFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description 保存用户
 * 
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class CollectHouseInfoByTransTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = -7504202359012982222L;

	public CollectHouseInfoByTransTopo(String topic, String topologyName) {
		super(topic, topologyName);
	}

	private static final String topic = "vt_trannjouextens";
	public static final String topologyName = "Calc_HouseInfoByTrans"
			+ new Random().nextInt(10000);

	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new CollectHouseInfoByTransTopo(topic, topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout(topic);
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		stream.each(new Fields(STR), new QueryDataByRedisFun(poolConfig),
				new Fields(DASConstants.DATA)).each(
				new Fields(DASConstants.DATA),
				new SaveHouseInfoByTransFun(poolConfig),new Fields());

		return topology.build();
	}
}