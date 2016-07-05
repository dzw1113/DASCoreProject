package com.icip.framework.topology.calc;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.aggregate.CalcAggregatorImpl;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.PrintResultFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description 历史欠费(bs_arreas_inf)
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalArreasAmountTopo extends AbstractBaseTopo {

	public TotalArreasAmountTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topologyName = "Calc_TotalArreasAmount_TOPO"
			+ new Random().nextInt(100);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalArreasAmountTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_arreas_info");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(5);
		
		Stream newStream = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig),
				new Fields(DASConstants.DATA)).parallelismHint(10);

		StateFactory incByHidFactory = getStrFactory(poolConfig);
		Stream hidStream = newStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_ArreasAmountByHid",
								"plotId", "houseId"), new Fields("newKey"))
								;
		hidStream.groupBy(new Fields("newKey"))
				.persistentAggregate(
						incByHidFactory,
						new Fields(DASConstants.DATA),
						new CalcAggregatorImpl("arrreasAmount", "payFlag", "1"),
						new Fields("arrreasAmount"));
		
		hidStream.each(new Fields(STR), new PrintResultFun(),new Fields());

//		StateFactory incByChargeNoFactory = getStrFactory(poolConfig);
//		newStream
//				.each(new Fields(DASConstants.DATA),
//						new ConcatRedisKeyFun("cal_ArreasAmountByChargeNo",
//								"plotId", "chargeNo"), new Fields("newKey"))
//				.groupBy(new Fields("newKey"))
//				.persistentAggregate(
//						incByChargeNoFactory,
//						new Fields(DASConstants.DATA),
//						new CalcAggregatorImpl("arrreasAmount", "payFlag", "1"),
//						new Fields("arrreasAmount"));
//
//		StateFactory incByPlotIdFactory = getStrFactory(poolConfig);
//		newStream
//				.each(new Fields(DASConstants.DATA),
//						new ConcatRedisKeyFun("cal_ArreasAmountByPlotId",
//								"plotId"), new Fields("newKey"))
//				.groupBy(new Fields("newKey"))
//				.persistentAggregate(
//						incByPlotIdFactory,
//						new Fields(DASConstants.DATA),
//						new CalcAggregatorImpl("arrreasAmount", "payFlag", "1"),
//						new Fields("arrreasAmount"));
//
//		StateFactory hcFactory = getStrFactory(poolConfig);
//		newStream
//				.each(new Fields(DASConstants.DATA),
//						new ConcatRedisKeyFun("cal_ArreasAmountByHidChargeNo",
//								"plotId", "houseId", "chargeNo"),
//					 	new Fields("newKey"))
//				.groupBy(new Fields("newKey"))
//				.persistentAggregate(
//						hcFactory,
//						new Fields(DASConstants.DATA),
//						new CalcAggregatorImpl("arrreasAmount", "payFlag", "1"),
//						new Fields("arrreasAmount"));

		return topology.build();
	}

}
