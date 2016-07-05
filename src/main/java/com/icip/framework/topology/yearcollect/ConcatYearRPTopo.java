package com.icip.framework.topology.yearcollect;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.common.QueryStrByRedisFun;
import com.icip.framework.function.yearcollect.GetOfYearCollectKeyFun;
import com.icip.framework.function.yearcollect.GetRealYearCollectKeyFun;
import com.icip.framework.function.yearcollect.GetRecYearCollectKeyFun;
import com.icip.framework.function.yearcollect.SaveRtYearKeyValFun;
import com.icip.framework.spout.RedisAckPubSubBaseSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

public class ConcatYearRPTopo extends AbstractBaseTopo {

	public ConcatYearRPTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	// topogy名称
	public static final String topologyName = "VT_YEAR_COLLECTION"
			+ new Random().nextInt(100);
	// 本地模式还是集群模式
	private static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new ConcatYearRPTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
//		RedisSpout spout = new RedisSpout("cal_receivedTotalByChargeNo:*:*:*:*");
		RedisAckPubSubBaseSpout spout = new RedisAckPubSubBaseSpout("cal_receivedTotalByChargeNo");
		Stream stream = topology.newStream(topologyName, spout);

		StateFactory fact = getStrFactory(poolConfig);
		Stream pStream = stream.each(new Fields(DASConstants.STR),
				new QueryStrByRedisFun(poolConfig), new Fields("amount")).each(
				new Fields(DASConstants.STR), new GetRecYearCollectKeyFun(),
				new Fields("reccy", "recc", "recy", "newKey"));

		// 所有费项汇总缴费金额按年
		// save cal_receivedTotalByChargeNoYear:cid/chargeNo/year
		TridentState state = pStream.groupBy(new Fields("reccy"))
				.persistentAggregate(fact, new Fields("amount"), new Sum(),
						new Fields("totalAmount"));
		pStream.stateQuery(state, new Fields("reccy"), new MapGet(),
				new Fields("totalAmount")).each(
				new Fields(DASConstants.STR, "totalAmount"),
				new SaveRtYearKeyValFun("receivedCcy"), new Fields());

		// 所有费项汇总缴费金额按小区 新增：annualAllReceivedAmount
		// save:cal_receivedTotalByYear:cid:chargeNo
		TridentState aystate = pStream.groupBy(new Fields("recc"))
				.persistentAggregate(fact, new Fields("amount"), new Sum(),
						new Fields("totalAmount"));
		pStream.stateQuery(aystate, new Fields("recc"), new MapGet(),
				new Fields("totalAmount")).each(
				new Fields(DASConstants.STR, "totalAmount"),
				new SaveRtYearKeyValFun("receivedCc"), new Fields());

		TridentState recystate = pStream.groupBy(new Fields("recy"))
				.persistentAggregate(fact, new Fields("amount"), new Sum(),
						new Fields("totalAmount"));
		pStream.stateQuery(recystate, new Fields("recy"), new MapGet(),
				new Fields("totalAmount")).each(
				new Fields(DASConstants.STR, "totalAmount"),
				new SaveRtYearKeyValFun("receivedCy"), new Fields());

		// -----------------------------start
		StateFactory realfact = getStrFactory(poolConfig);
		Stream rStream = pStream.each(new Fields("newKey"),
				new QueryStrByRedisFun(poolConfig), new Fields("ramount"))
				.each(new Fields(DASConstants.STR),
						new GetRealYearCollectKeyFun(),
						new Fields("realccy", "realcc", "realcy"));
		// 全年费项实收金额
		TridentState rastate = rStream.groupBy(new Fields("realccy"))
				.persistentAggregate(realfact, new Fields("ramount"),
						new Sum(), new Fields("totalrAmount"));
		rStream.stateQuery(rastate, new Fields("realccy"), new MapGet(),
				new Fields("totalrAmount")).each(
				new Fields("newKey", "totalrAmount"),
				new SaveRtYearKeyValFun("realMountCcy"), new Fields());

		TridentState raystate = rStream.groupBy(new Fields("realcc"))
				.persistentAggregate(realfact, new Fields("ramount"),
						new Sum(), new Fields("totalrAmount"));
		rStream.stateQuery(raystate, new Fields("realcc"), new MapGet(),
				new Fields("totalrAmount")).each(
				new Fields("newKey", "totalrAmount"),
				new SaveRtYearKeyValFun("realMountCc"), new Fields());

		TridentState rcystate = rStream.groupBy(new Fields("realcy"))
				.persistentAggregate(realfact, new Fields("ramount"),
						new Sum(), new Fields("totalrAmount"));
		rStream.stateQuery(rcystate, new Fields("realcy"), new MapGet(),
				new Fields("totalrAmount")).each(
				new Fields("newKey", "totalrAmount"),
				new SaveRtYearKeyValFun("realMountCy"), new Fields());

		// ----滞纳金-----------start
		// cal_offsetTotalByChargeNo:C2220000000148:C001047:2016:03
		Stream tastream = pStream.each(new Fields(DASConstants.STR),
				new GetOfYearCollectKeyFun(),
				new Fields("ofcc", "ofcy", "ofKey")).each(new Fields("ofKey"),
				new QueryStrByRedisFun(poolConfig), new Fields("ofamount"));

		StateFactory ofsfact = getStrFactory(poolConfig);
		TridentState ofstate = tastream.groupBy(new Fields("ofcc"))
				.persistentAggregate(ofsfact, new Fields("ofamount"),
						new Sum(), new Fields("totalOfAmount"));

		tastream.stateQuery(ofstate, new Fields("ofcc"), new MapGet(),
				new Fields("totalOfAmount")).each(
				new Fields("ofKey", "totalOfAmount"),
				new SaveRtYearKeyValFun("offsetCc"), new Fields());

		TridentState taystate = tastream.groupBy(new Fields("ofcy"))
				.persistentAggregate(ofsfact, new Fields("ofamount"),
						new Sum(), new Fields("totalcyAmount"));

		tastream.stateQuery(taystate, new Fields("ofcy"), new MapGet(),
				new Fields("totalcyAmount")).each(
				new Fields("ofKey", "totalcyAmount"),
				new SaveRtYearKeyValFun("offsetCy"), new Fields());

		return topology.build();
	}
}
