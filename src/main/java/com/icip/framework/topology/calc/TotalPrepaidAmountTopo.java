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
import com.icip.framework.aggregate.SumAggregatorImpl;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.filter.EqValBaseFilter;
import com.icip.framework.function.JournalsConcatMapFun;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisAckPubSubBaseSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description 计算预收总和(bs_trans_journals,bs_business_journals) 按 cid/year/month
 *              cid/hid/year/month cid/chargeNo/year/month
 *              cid/hid/chargeNo/year/month mark--------------->
 *              VT_TotalPrepaidAmount保存的businessAmount为单户单月单费项预缴总和
 * @author
 * @date 2016年3月30日 上午10:04:39
 * @update
 */
public class TotalPrepaidAmountTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = -7504202359012982222L;

	public TotalPrepaidAmountTopo(String topologyName) {
		super(topologyName);
	}

	public static final String topic = "bs_business_journals";
	public static final String topologyName = "Calc_TotalPrepaidAmount"
			+ new Random().nextInt(10000);
	public static final String spoutId = "vt_prepaid_c"
				+ new Random().nextInt(10000);
	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisAckPubSubBaseSpout spout = new RedisAckPubSubBaseSpout("bs_business_journals");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		Stream streamTotal = stream
				.each(new Fields(STR), new QueryDataByRedisFun(poolConfig),
						new Fields("businessJournalData")).parallelismHint(1)
				.each(new Fields("businessJournalData"),
						new ConcatRedisKeyFun("bs_trans_journals", "transId"),
						new Fields("transIdKey"))
				.each(new Fields("transIdKey"),
						new QueryDataByRedisFun(poolConfig),
						new Fields("transJournalData")).parallelismHint(1)
				.each(new Fields("transJournalData"),
								new EqValBaseFilter("transChargeType", "10"))
				.each(new Fields("transJournalData"),
								new EqValBaseFilter("transStatus", "03"))//只取trans_charge_type=10，trans_status=03	
				.each(new Fields("businessJournalData", "transJournalData"), new JournalsConcatMapFun(),
						new Fields(DASConstants.DATA));
		// -----预缴 cid/year/month统计
		StateFactory prepaidFactByCidFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_prepaidTotalByCid",
								new String[] { "cid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(prepaidFactByCidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("prepaidFactByCid"));

		// -----预缴 cid/hid/year/month
		StateFactory prepaidFactByHidFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_prepaidTotalByHid",
								new String[] { "cid", "hid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(prepaidFactByHidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("prepaidFactByHid"));

		// -----预缴 cid/chargeNo/year/month
		StateFactory prepaidFactByChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_prepaidTotalByChargeNo",
								new String[] { "cid", "chargeNo", "year",
										"month" }), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(prepaidFactByChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("prepaidFactByChargeNo"));

		// -----预缴 cid/hid/chargeNo/year/month
		StateFactory prepaidFactByHidAndChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_prepaidTotalByHidAndChargeNo",
								new String[] { "cid", "hid", "chargeNo",
										"year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(prepaidFactByHidAndChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("businessAmount"),
						new Fields("prepaidFactByHidAndChargeNo"));

		return topology.build();
	}
	
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalPrepaidAmountTopo(topologyName).execute(flag);
	}

}