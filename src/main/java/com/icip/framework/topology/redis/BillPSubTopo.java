package com.icip.framework.topology.redis;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import scala.util.Random;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisAckPubSubBaseSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;
/**
 *	对应 vt_bill
 */
public class BillPSubTopo extends AbstractBaseTopo {

	public BillPSubTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "bs_bill_details";
	public static final String topologyName = "psub_subBill"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new BillPSubTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {
		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisAckPubSubBaseSpout spout = new RedisAckPubSubBaseSpout("bs_bill_details");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(100);

		stream.each(new Fields(STR), new QueryDataByRedisFun(poolConfig),
				new Fields("detailData"))
				.each(new Fields(STR,"detailData"), new QueryBillByRedisFun(poolConfig),
						new Fields());

		return topology.build();
	}
}
