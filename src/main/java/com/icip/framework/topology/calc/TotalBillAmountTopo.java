package com.icip.framework.topology.calc;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import scala.util.Random;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.aggregate.SumAggregatorImpl;
import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.PrintResultFun;
import com.icip.framework.spout.RedisAckPubSubBaseSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;
import com.icip.framework.topology.redis.QueryDataBySTR;

/**
 * @Description: 计算账单中应交款,已交款，尚欠款(bs_pay_bills,bs_bill_details) 按 cid/year/month
 *               cid/chargeNo/year/month cid/hid/year/month
 *               cid/hid/chargeNo/year/month分组
 * @author
 * @date 2016年3月31日 下午1:55:43
 * @update
 */
public class TotalBillAmountTopo extends AbstractBaseTopo {

	public TotalBillAmountTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_bill";
	public static final String topologyName = "Calc_TotalBillAmount"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new TotalBillAmountTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisAckPubSubBaseSpout spout = new RedisAckPubSubBaseSpout("vt_bill");
		Stream streamTotal = topology.newStream(topologyName, spout).each(new Fields(STR),
				new QueryDataBySTR(),
				new Fields(DASConstants.DATA));
		

		// -----账单中应交金额统计 cid/year/month
		StateFactory payableTotalByCidFact = getStrFactory(poolConfig);
		streamTotal = streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_payableTotalByCid",
								new String[] { "plotId", "year", "month" }),
						new Fields("newKey")).parallelismHint(1);
		TridentState paymentState =		streamTotal.groupBy(new Fields("newKey"))
				.persistentAggregate(payableTotalByCidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("paymentAmount"),
						new Fields("payableTotalByCid"));
		streamTotal.stateQuery(paymentState, new Fields("newKey"),
				new MapGet(), new Fields("payableTotalByCid"))
				.each(new Fields(), new PrintResultFun(),new Fields());
		

		// -----账单中应交金额统计 cid/chargeNo/year/month
		/*StateFactory payableTotalByChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_payableTotalByChargeNo",
								new String[] { "plotId", "chargeNo", "year",
										"month" }), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(payableTotalByChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("payableAmount"),
						new Fields("payableTotalByChargeNo"));

		// -----账单中应交金额统计 cid/hid/year/month
		StateFactory payableTotalByHid = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_payableTotalByHid",
								new String[] { "plotId", "hid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(payableTotalByHid,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("payableAmount"),
						new Fields("payableTotalByHid"));

		// -----账单中应交金额统计 cid/hid/chargeNo/year/month
		StateFactory payableTotalByHidAndChargeNo = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_payableTotalByHidAndChargeNo",
								new String[] { "plotId", "hid", "chargeNo",
										"year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(payableTotalByHidAndChargeNo,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("payableAmount"),
						new Fields("payableTotalByHidAndChargeNo"));

		// -----账单中已交金额统计 cid/year/month
		StateFactory receivedTotalByCidFact = getStrFactory(poolConfig);
		TridentState receivedState = streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_receivedTotalByCid",
								new String[] { "plotId", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(receivedTotalByCidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("receivedAmount"),
						new Fields("receivedTotalByCid"));

		// -----账单中已交金额统计 cid/chargeNo/year/month
		StateFactory receivedTotalByChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_receivedTotalByChargeNo",
								new String[] { "plotId", "chargeNo", "year",
										"month" }), new Fields("newKey"))
				.each(new Fields("newKey"), new RedisPublishFun(),new Fields())
				.groupBy(new Fields("newKey"))
				.persistentAggregate(receivedTotalByChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("receivedAmount"),
						new Fields("receivedTotalByChargeNo"));
		

		// -----账单中已交金额统计 cid/hid/year/month
		StateFactory receivedTotalByHid = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_receivedTotalByHid",
								new String[] { "plotId", "hid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(receivedTotalByHid,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("receivedAmount"),
						new Fields("receivedTotalByHid"));

		// -----账单中已交金额统计 cid/hid/chargeNo/year/month
		StateFactory receivedToalByHidAndChargeNo = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_receivedTotalByHidAndChargeNo",
								new String[] { "plotId", "hid", "chargeNo",
										"year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(receivedToalByHidAndChargeNo,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("receivedAmount"),
						new Fields("receivedTotalByHidAndChargeNo"));

		// -----账单中尚欠金额统计 cid/year/month
		StateFactory arrearsTotalByCidFact = getStrFactory(poolConfig);
		TridentState arrearsState = streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_arrearsTotalByCid",
								new String[] { "plotId", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrearsTotalByCidFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrears"),
						new Fields("arrearsTotalByCid"));

		// -----账单中尚欠金额统计 cid/chargeNo/year/month
		StateFactory arrearsTotalByChargeNoFact = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_arrearsTotalByChargeNo",
								new String[] { "plotId", "chargeNo", "year",
										"month" }), new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrearsTotalByChargeNoFact,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrears"),
						new Fields("arrearsTotalByChargeNo"));

		// -----账单中尚欠金额统计 cid/hid/year/month
		StateFactory arrearsTotalByHid = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_arrearsTotalByHid",
								new String[] { "plotId", "hid", "year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrearsTotalByHid,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrears"),
						new Fields("arrearsTotalByHid"));

		// -----账单中尚欠金额统计 cid/hid/chargeNo/year/month
		StateFactory arrearsToalByHidAndChargeNo = getStrFactory(poolConfig);
		streamTotal
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_arrearsTotalByHidAndChargeNo",
								new String[] { "plotId", "hid", "chargeNo",
										"year", "month" }),
						new Fields("newKey"))
				.groupBy(new Fields("newKey"))
				.persistentAggregate(arrearsToalByHidAndChargeNo,
						new Fields(DASConstants.DATA),
						new SumAggregatorImpl("arrears"),
						new Fields("arrearsTotalByHidAndChargeNo"));

		if (flag) {
			topology.newDRPCStream("queryAmount")
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_payableTotalByCid"),
							new Fields("payableKey"))
					.stateQuery(paymentState, new Fields("payableKey"),
							new MapGet(), new Fields("payableTotalByCid"))
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_receivedTotalByCid"),
							new Fields("receivedKey"))
					.stateQuery(receivedState, new Fields("receivedKey"),
							new MapGet(), new Fields("receivedTotalByCid"))
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_arrearsTotalByCid"),
							new Fields("arrearsKey"))
					.stateQuery(arrearsState, new Fields("arrearsKey"),
							new MapGet(), new Fields("arrearsTotalByCid"))
					.aggregate(
							new Fields("payableTotalByCid",
									"receivedTotalByCid", "arrearsTotalByCid"),
							new DrpcResultAggregate(),
							new Fields("result1", "result2", "result3"));
		} else {
			topology.newDRPCStream("queryAmount", drpc)
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_payableTotalByCid"),
							new Fields("payableKey"))
					.stateQuery(paymentState, new Fields("payableKey"),
							new MapGet(), new Fields("payableTotalByCid"))
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_receivedTotalByCid"),
							new Fields("receivedKey"))
					.stateQuery(receivedState, new Fields("receivedKey"),
							new MapGet(), new Fields("receivedTotalByCid"))
					.each(new Fields("args"),
							new DRPCConcatTbKeyFun("cal_arrearsTotalByCid"),
							new Fields("arrearsKey"))
					.stateQuery(arrearsState, new Fields("arrearsKey"),
							new MapGet(), new Fields("arrearsTotalByCid"))
					.aggregate(
							new Fields("payableTotalByCid",
									"receivedTotalByCid", "arrearsTotalByCid"),
							new DrpcResultAggregate(),
							new Fields("result1", "result2", "result3"));
		}
*/
		return topology.build();
	}

}
