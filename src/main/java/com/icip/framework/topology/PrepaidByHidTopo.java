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
import com.icip.framework.filter.EqValBaseFilter;
import com.icip.framework.function.HandlePrepaidByHidFieldsFun;
import com.icip.framework.function.JournalsConcatMapFun;
import com.icip.framework.function.common.ConcatRedisKeyFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/** 
 * @Description: 预收统计(房间项目预收)
 * @author  
 * @date 2016年3月30日 上午10:24:05 
 * @update	
 */
public class PrepaidByHidTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = 8178403180947719696L;

	public PrepaidByHidTopo(String topologyName) {
		super(topologyName);
	}

	// topogy名称
	public static final String topologyName = "vt_prepaidByHidTopo"
			+ new Random().nextInt(10);

	// false本地 true线上
	private static boolean flag = false;

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {
		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("bs_business_journals");
		Stream spoutStream = topology.newStream(topologyName, spout).parallelismHint(1);

		spoutStream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						"businessJournalData"))
				.each(new Fields("businessJournalData"),
						new ConcatRedisKeyFun("bs_trans_journals", "transId"),
						new Fields("transIdKey"))
				.each(new Fields("transIdKey"),
						new QueryDataByRedisFun(poolConfig),
						new Fields("transJournalData"))
				.each(new Fields("transJournalData"),
								new EqValBaseFilter("transChargeType", "10"))
				.each(new Fields("transJournalData"),
								new EqValBaseFilter("transStatus", "03"))		
				.each(new Fields("businessJournalData", "transJournalData"), new JournalsConcatMapFun(),
						new Fields(DASConstants.DATA))
				//处理
				.each(new Fields(DASConstants.DATA), new HandlePrepaidByHidFieldsFun(poolConfig),new Fields());		
		
//		--------------------------------下列注释误删----------------------------------------->
		
//		stream
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("cid"), new Fields("cid"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("hid"), new Fields("hid"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("year"), new Fields("year"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("month"), new Fields("month"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("roomNo"), new Fields("roomNo"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("pname"), new Fields("ownerName"))
//				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("chargeNo"), new Fields("chargeNo"))
//				.each(new Fields(DASConstants.DATA), new QueryFieldsByDataFun("cid","hid","chargeNo","year","month"), new Fields("hidChargeKey"))
//				.each(new Fields(DASConstants.DATA), new QueryFieldsByDataFun("cid","hid","year","month"), new Fields("hidKey"));
//				
//		stream
//				//预收金额合计
//				.each(new Fields("hidKey"), new QueryCalDataFun(poolConfig,
//					   "cal_prepaidTotalByHid"),new Fields("totalPrepaidAmount"))
//				//取抵充金额合计
//			    .each(new Fields("hidKey"), new QueryCalDataFun(poolConfig,
//					   "cal_offsetTotalByHid"),new Fields("totalOffsetAmount"))
//				//取应交款总计
//				.each(new Fields("hidKey"), new QueryCalDataFun(poolConfig,
//					   "cal_payableTotalByHid"),new Fields("totalPayableAmount"));
//		stream
//				//取预缴表余额总和(期末余额)
//				.each(new Fields(DASConstants.DATA), new QueryPrepaidAmount(poolConfig),
//						new Fields("currentAmount"))
//				//根据费项取预收
//				.each(new Fields("hidChargeKey"), new QueryCalDataFun(poolConfig,
//						"cal_prepaidTotalByHidAndChargeNo"),new Fields("chargeAmount"))
//				//根据费项取应收
//				.each(new Fields("hidChargeKey"), new QueryCalDataFun(poolConfig,
//						"cal_payableTotalByHidAndChargeNo"),new Fields("payableAmount"))
//				//根据费项取冲销
//				.each(new Fields("hidChargeKey"), new QueryCalDataFun(poolConfig,
//						"cal_offsetTotalByHidAndChargeNo"),new Fields("offsetAmount"))	
//						.each(new Fields("offsetAmount"), new PrintResultFun(),new Fields())		
//				//上期余额
//				.each(new Fields("currentAmount","chargeAmount","offsetAmount"),
//						new PreviousBlanceFun(),
//						new Fields("previousBlance"));
//		stream		
//				//保存结果
//				.each(new Fields("cid","hid","chargeNo","year","month","roomNo","ownerName","previousBlance",
//							"chargeAmount","offsetAmount","payableAmount","currentAmount",
//							"totalPrepaidAmount","totalOffsetAmount","totalPayableAmount"),
//						new SavePrepaidByHidFun(poolConfig,
//						new String[]{"cid","hid","chargeNo","year","month","roomNo","ownerName","previousBlance",
//								"chargeAmount","offsetAmount","payableAmount","currentAmount",
//								"totalPrepaidAmount","totalOffsetAmount","totalPayableAmount"}),new Fields());
		return topology.build(); 
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new PrepaidByHidTopo(topologyName).execute(flag);
	}

}
