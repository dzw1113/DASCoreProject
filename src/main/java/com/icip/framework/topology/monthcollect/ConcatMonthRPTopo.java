package com.icip.framework.topology.monthcollect;

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
import com.icip.framework.function.ReserveKeyFunction;
import com.icip.framework.function.ReservePreMonKeyFunction;
import com.icip.framework.function.TotalArrFun;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.function.common.QueryKeyByRedisFun;
import com.icip.framework.function.common.QueryStrByRedisFun;
import com.icip.framework.function.monthcollect.SaveMonthBillAmountFun;
import com.icip.framework.function.monthcollect.SaveMonthKeyValAmountFun;
import com.icip.framework.spout.RedisSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

public class ConcatMonthRPTopo extends AbstractBaseTopo {

	public ConcatMonthRPTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	// topogy名称
	public static final String topologyName = "VT_THIS_MONTH_COLLECTION"
			+ new Random().nextInt(100);
	// 本地模式还是集群模式
	private static boolean flag = false;

	public static void main(String[] args) throws Exception {

		if (args != null && args.length > 0) {
			flag = true;
		}
		new ConcatMonthRPTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisSpout spout = new RedisSpout("vt_houseinfo:*:*:*:*:*");
		// RedisSpout spout = new RedisSpout(
		// "vt_houseinfo:C2230000000181:H0000072106:*:2016:03");
		Stream stream = topology.newStream(topologyName, spout);

		Stream dataStream = stream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						DASConstants.DATA));
		// 账单收
		dataStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_payableTotalByHidAndChargeNo", "plotId",
								"hid", "chargeNo", "year", "month"),
						new Fields("billkey"))
				// 本月账单收
				.each(new Fields("billkey"),
						new QueryStrByRedisFun(poolConfig),
						new Fields("chargeAmount"))
				// ---------------------
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_payableTotalByHid",
								"plotId", "hid", "year", "month"),
						new Fields("bpkey"))
				// 本月账单收小计
				.each(new Fields("bpkey"), new QueryStrByRedisFun(poolConfig),
						new Fields("totalChargeAmount"))
				// ---------------------------------------
				// 实收
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_RealAmountByHidChargeNo",
								"plotId", "hid", "chargeNo", "year", "month"),
						new Fields("rakey"))
				// 本月收
				.each(new Fields("rakey"), new QueryStrByRedisFun(poolConfig),
						new Fields("thisMonthAmount"))
				// --------------
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_RealAmountByHid", "plotId",
								"hid", "year", "month"), new Fields("ratKey"))
				// 小计
				.each(new Fields("ratKey"), new QueryStrByRedisFun(poolConfig),
						new Fields("totalThisMonthAmount"))
				// ---------------------------------------
				// 本月补收
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByHidChargeNo",
								"plotId", "hid", "chargeNo", "year", "month"),
						new Fields("iakey"))
				.each(new Fields("iakey"), new QueryStrByRedisFun(poolConfig),
						new Fields("incomeAmount"))
				// ---
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_IncomeAmountByHid",
								"plotId", "hid", "year", "month"),
						new Fields("tiakey"))
				// 小计
				.each(new Fields("tiakey"), new QueryStrByRedisFun(poolConfig),
						new Fields("totalIncomeAmount"))
				// ---------------------------------------
				// 本月预收
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_prepaidTotalByHidAndChargeNo", "plotId",
								"hid", "chargeNo", "year", "month"),
						new Fields("ptkey"))
				.each(new Fields("ptkey"), new QueryStrByRedisFun(poolConfig),
						new Fields("prepaidAmount"))
				// ----
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_prepaidTotalByHid",
								"plotId", "hid", "year", "month"),
						new Fields("tpakey"))

				// 小计
				.each(new Fields("tpakey"), new QueryStrByRedisFun(poolConfig),
						new Fields("totalPrepaidAmount"))
				// ---------------------------------------
				// 违约金
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_LateFeeByHid", "plotId",
								"hid", "year", "month"), new Fields("lfkey"))
				// 小计
				.each(new Fields("lfkey"), new QueryStrByRedisFun(poolConfig),
						new Fields("totalLateFee"))

				// ---------------------------------------
				// 当月欠费
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo", "year", "month"),
						new Fields("baakey"))
				// 小计
				.each(new Fields("baakey"), new QueryStrByRedisFun(poolConfig),
						new Fields("arrears"))

				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_BillArreasAmountByHid",
								"plotId", "hid", "year", "month"),
						new Fields("tbaakey"))
				// 小计
				.each(new Fields("tbaakey"),
						new QueryStrByRedisFun(poolConfig),
						new Fields("totalArrears"))

				// -----------------------保存结果1
				.each(new Fields(DASConstants.DATA, "chargeAmount",
						"totalChargeAmount", "thisMonthAmount",
						"totalThisMonthAmount", "incomeAmount",
						"totalIncomeAmount", "prepaidAmount",
						"totalPrepaidAmount", "totalLateFee", "arrears",
						"totalArrears"),
						new SaveMonthBillAmountFun(poolConfig), new Fields());

		// ---------------------------------------
		StateFactory factoryRt = getStrFactory(poolConfig);
		// 上月止欠费
		TridentState preMonthstate = dataStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo"), new Fields("mkey"))
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo", "*", "*"),
						new Fields("pattern"))

				.each(new Fields("pattern"),
						new QueryKeyByRedisFun(poolConfig), new Fields("rkey"))
				.each(new Fields("rkey", DASConstants.DATA),
						new ReservePreMonKeyFunction(),
						new Fields("reserveKey"))
				.each(new Fields("reserveKey"),
						new QueryStrByRedisFun(poolConfig),
						new Fields("amount"))
				.groupBy(new Fields("mkey"))
				.persistentAggregate(factoryRt, new Fields("amount"),
						new Sum(), new Fields("historyArrears"));
		// 合计欠费
		Stream nbtData = dataStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo"), new Fields("braamkey"))
				.stateQuery(preMonthstate, new Fields("braamkey"),
						new MapGet(), new Fields("historyArrears"))
				// .each(new Fields(DASConstants.DATA, "historyArrears"),
				// new SaveMonthKeyValAmountFun(poolConfig,
				// "historyArrears"), new Fields())
				// 上月止欠费
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_ArreasAmountByHidChargeNo",
								"plotId", "hid", "chargeNo"),
						new Fields("tbarrkey"))
				.each(new Fields("tbarrkey"),
						new QueryStrByRedisFun(poolConfig),
						new Fields("arrAMount"))
				.each(new Fields("historyArrears", "arrAMount"),
						new TotalArrFun(), new Fields("totalHistoryArreas"));

		nbtData.each(new Fields(DASConstants.DATA, "historyArrears"),
				new SaveMonthKeyValAmountFun(poolConfig, "historyArrears"),
				new Fields());
		nbtData.each(new Fields(DASConstants.DATA, "totalHistoryArreas"),
				new SaveMonthKeyValAmountFun(poolConfig, "totalHistoryArreas"),
				new Fields());

		// ---------------------------------------
		StateFactory factoryAll = getStrFactory(poolConfig);
		// 累计欠费
		TridentState state = dataStream
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo"), new Fields("mkey"))
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo", "*", "*"),
						new Fields("pattern"))
				.each(new Fields("pattern"),
						new QueryKeyByRedisFun(poolConfig), new Fields("rkey"))
				.each(new Fields("rkey", DASConstants.DATA),
						new ReserveKeyFunction(), new Fields("reserveKey"))
				.each(new Fields("reserveKey"),
						new QueryStrByRedisFun(poolConfig),
						new Fields("amount"))
				.groupBy(new Fields("mkey"))
				.persistentAggregate(factoryAll, new Fields("amount"),
						new Sum(), new Fields("allArreas"));
		// 合计欠费
		Stream sarrea = nbtData
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun(
								"cal_BillArreasAmountByHidChargeNo", "plotId",
								"hid", "chargeNo"), new Fields("mkey"))
				.stateQuery(state, new Fields("mkey"), new MapGet(),
						new Fields("allArreas"))
				// 历史欠费
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("cal_ArreasAmountByHidChargeNo",
								"plotId", "hid", "chargeNo"),
						new Fields("arrkey"))
				.each(new Fields("arrkey"), new QueryStrByRedisFun(poolConfig),
						new Fields("arrAllAMount"))
				.each(new Fields("allArreas", "arrAllAMount"),
						new TotalArrFun(), new Fields("allArreasSum"));

		sarrea.each(new Fields(DASConstants.DATA, "allArreas"),
				new SaveMonthKeyValAmountFun(poolConfig, "allArreas"),
				new Fields());
		sarrea.each(new Fields(DASConstants.DATA, "allArreasSum"),
				new SaveMonthKeyValAmountFun(poolConfig, "allArreasSum"),
				new Fields());

		return topology.build();
	}
}
