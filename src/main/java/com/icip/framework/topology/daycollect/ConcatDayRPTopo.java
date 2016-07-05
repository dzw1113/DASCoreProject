package com.icip.framework.topology.daycollect;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.aggregate.SumAggregatorImpl;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.function.daycollect.GenDayKeyFun;
import com.icip.framework.function.daycollect.SaveDayBillAmountFun;
import com.icip.framework.function.daycollect.SaveDayHidBillAmountFun;
import com.icip.framework.topology.base.AbstractBaseTopo;

public class ConcatDayRPTopo extends AbstractBaseTopo {

	public ConcatDayRPTopo(String topologyName) {
		super(topic,topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_trans_detail";
	
	// topogy名称
	public static final String topologyName = "VT_ConcatDayRPTopo_TOPO"
			+ new Random().nextInt(100);
	// 本地模式还是集群模式
	private static boolean flag = false;

	public static void main(String[] args) throws Exception {

		if (args != null && args.length > 0) {
			flag = true;
		}
		new ConcatDayRPTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		TransactionalTridentKafkaSpout kafkaSpout = getSpout();
//		RedisSpout kafkaSpout = new RedisSpout(
//				"vt_trans_detail:ICIPB201602290000000102*");
		Stream stream = topology.newStream(topologyName, kafkaSpout);

		// --------------------筛选

		Stream dataStream = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig),
				new Fields(DASConstants.DATA)).each(
				new Fields(DASConstants.DATA), new GenDayKeyFun(),
				new Fields("hdayByHid", "hdayByChargeNo", "hdayByHC"));

		// 根据房屋分组
		StateFactory hdayByhState = getFactory(poolConfig, "hdayByHid");
		TridentState hstate = dataStream.groupBy(new Fields("hdayByHid"))
				.persistentAggregate(hdayByhState,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("transAmount"),
						new Fields("hidTransAmount"));
		// 根据费项分组
		StateFactory hdayByCState = getFactory(poolConfig, "hdayByChargeNo");
		TridentState cstate = dataStream.groupBy(new Fields("hdayByChargeNo"))
				.persistentAggregate(hdayByCState,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("transAmount"),
						new Fields("chargeTransAmount"));
		// 根据房屋费项汇总
		StateFactory hdayByHCState = getFactory(poolConfig, "hdayByHC");
		TridentState hcstate = dataStream.groupBy(new Fields("hdayByHC"))
				.persistentAggregate(hdayByHCState,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("transAmount"),
						new Fields("hcTransAmount"));

		// 保存日记账明细
		dataStream.each(new Fields(STR, DASConstants.DATA),
				new SaveDayBillAmountFun(poolConfig), new Fields());

		// 保存日记账明细（按房间）
		dataStream
				.stateQuery(hstate, new Fields("hdayByHid"), new MapGet(),
						new Fields("hidTransAmount"))
				.stateQuery(cstate, new Fields("hdayByChargeNo"), new MapGet(),
						new Fields("chargeTransAmount"))
				.stateQuery(hcstate, new Fields("hdayByHC"), new MapGet(),
						new Fields("hcTransAmount"))
				.each(new Fields(STR, DASConstants.DATA, "hidTransAmount",
						"chargeTransAmount", "hcTransAmount"),
						new SaveDayHidBillAmountFun(poolConfig), new Fields());

		return topology.build();
	}
}
