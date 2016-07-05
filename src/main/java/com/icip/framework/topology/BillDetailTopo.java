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
import com.icip.framework.function.BillConcatMapFun;
import com.icip.framework.function.QueryChargeUnitFun;
import com.icip.framework.function.SaveBillDetailFun;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryChargeNameByChargeNo;
import com.icip.framework.function.common.QueryDataByRedisAllowBlankFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.function.common.QueryFieldsByDataFun;
import com.icip.framework.spout.RedisAckPubSubBaseSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/**
 * @Description: 账单明细
 * @author
 * @date 2016年3月28日 下午7:51:41
 * @update
 */
public class BillDetailTopo extends AbstractBaseTopo {

	public BillDetailTopo(String topic, String spoutId, String topologyName) {
		super(topic, spoutId, topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "bs_bill_details";

	// 消费者唯一标示
	public static final String spoutId = "vt_bill_c"
			+ new Random().nextInt(10000);

	// topogy名称
	public static final String topologyName = "Calc_billDetailTopo_TOPO"
			+ new Random().nextInt(1000);

	// false本地 true线上
	private static boolean flag = false;

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisAckPubSubBaseSpout spout = new RedisAckPubSubBaseSpout("bs_business_journals");
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		// TODO 补充数据格式文档
		stream.each(new Fields(STR), new QueryDataByRedisFun(poolConfig),
				new Fields("detailData")).parallelismHint(3)
				.each(new Fields("detailData"),
						new ConcatRedisKeyFun("bs_pay_bills", "billNo"),
						new Fields("billNoKey"))
				.each(new Fields("billNoKey"),
						new QueryDataByRedisFun(poolConfig),
						new Fields("billData")).parallelismHint(3)
				.each(new Fields("billData", "detailData"),
						new BillConcatMapFun(), new Fields(DASConstants.DATA))
				//取费项说明
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("vt_charge_mark", "chargeNo",
								"hid"), new Fields("houseChargeKey"))
				.each(new Fields("houseChargeKey"),
						new QueryDataByRedisAllowBlankFun(poolConfig),
						new Fields("houseChargeMap")).parallelismHint(3)
				.each(new Fields("houseChargeMap"),
						new QueryFieldsByDataFun("chargeMark"),
						new Fields("explanation"))
				// 取费项单位
				.each(new Fields(DASConstants.DATA),
						new ConcatRedisKeyFun("vt_valuation_fomula", "hid",
								"chargeNo"), new Fields("valuationKey"))
				.each(new Fields("valuationKey"),
						new QueryDataByRedisAllowBlankFun(poolConfig),
						new Fields("valuationMap")).parallelismHint(3)
				.each(new Fields("valuationMap"),
						new QueryFieldsByDataFun("valuationFomula"),
						new Fields("valuationFomula"))
				.each(new Fields("valuationFomula"),
						new QueryChargeUnitFun(poolConfig), new Fields("unit")).parallelismHint(3)
				// 取费项名称
				.each(new Fields(DASConstants.DATA),
						new QueryFieldsByDataFun("chargeNo"),
						new Fields("chargeNo"))
				.each(new Fields("chargeNo"),
						new QueryChargeNameByChargeNo(poolConfig),
						new Fields("chargeName")).parallelismHint(3)
				// 保存结果
				.each(new Fields(DASConstants.DATA, "explanation", "unit",
						"chargeName"),
						new SaveBillDetailFun(poolConfig, new String[] {
								"explanation", "unit", "chargeName" }),
						new Fields());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new BillDetailTopo(topic, spoutId, topologyName).execute(flag);
	}

}
