package com.icip.framework.topology;

import java.util.Random;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.function.CalMoneySubtractFun;
import com.icip.framework.function.QueryBillDataFun;
import com.icip.framework.function.QueryCalDataFun;
import com.icip.framework.function.QueryPrepaidInfoFun;
import com.icip.framework.function.SavePrepaidOffsetDetailFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.function.common.QueryFieldsByDataFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/** 
 * @Description: 預收款查询（冲销明细）
 * @author  
 * @date 2016年3月30日 下午3:14:46 
 * @update	
 */
public class PrepaidOffsetDetailTopo extends AbstractBaseTopo {

	private static final long serialVersionUID = 8178403180947719696L;

	public PrepaidOffsetDetailTopo(String topologyName) {
		super(topologyName);
	}

	// topogy名称
	public static final String topologyName = "offsetDetailTopo"
			+ new Random().nextInt(10);

	// false本地 true线上
	private static boolean flag = false;

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {
		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout("vt_totalOffsetAmount");
		Stream spoutStream = topology.newStream(topologyName, spout);

		Stream stream = spoutStream.each(new Fields(STR),
				new QueryDataByRedisFun(poolConfig), new Fields(
						"offsetData"))
				//计算冲销后预收余额
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("plotId","hid","chargeNo","year","month"), new Fields("hidChargeKey"))
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("plotId"),new Fields("cid"))
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("hid"),new Fields("hid"))
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("chargeNo"),new Fields("chargeNo"))
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("year"),new Fields("year"))
				.each(new Fields("offsetData"), new QueryFieldsByDataFun("month"),new Fields("month"))
				.each(new Fields("hidChargeKey"),new QueryPrepaidInfoFun(poolConfig),new Fields("prepaidAmount"))		
				//取费项冲销金额	
				.each(new Fields("hidChargeKey"), new QueryCalDataFun(poolConfig,
						"cal_offsetTotalByHidAndChargeNo"),new Fields("offsetAmount"))
				//取账单
				.each(new Fields("hidChargeKey"), new QueryBillDataFun(poolConfig),new Fields("billDataMap"))
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("roomNo"), new Fields("roomNo"))		
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("chargeName"), new Fields("chargeName"))	
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("ownerName"), new Fields("ownerName"))	
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("chargeDuring"), new Fields("chargeDuring"))
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("paymentAmount"), new Fields("paymentAmount"))	
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("receivedAmount"), new Fields("receivedAmount"))	
				.each(new Fields("billDataMap"),new QueryFieldsByDataFun("arrears"), new Fields("arrears"))

				//计算冲销后尚欠金额
				.each(new Fields("arrears","offsetAmount"),new CalMoneySubtractFun(), new Fields("arrearsAfterOffset"))
				//取费项冲销前预收款
				.each(new Fields("hidChargeKey"), new QueryCalDataFun(poolConfig,
					   "cal_prepaidTotalByHidAndChargeNo"),new Fields("perpaidBeforOffset"));
				
		//保存结果 cid/hid/chargeNo/year/month
		stream.each(new Fields("cid","hid","chargeNo","year","month","roomNo","ownerName","chargeName",
				"chargeDuring","paymentAmount","receivedAmount","arrears","perpaidBeforOffset","offsetAmount",
				"arrearsAfterOffset","prepaidAmount"),
				new SavePrepaidOffsetDetailFun(poolConfig,
						new String[]{"cid","hid","chargeNo","year","month","roomNo","ownerName","chargeName",
						"chargeDuring","paymentAmount","receivedAmount","arrears","perpaidBeforOffset","offsetAmount",
						"arrearsAfterOffset","prepaidAmount"}),new Fields());
		return topology.build(); 
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new PrepaidOffsetDetailTopo(topologyName).execute(flag);
	}
	
}
