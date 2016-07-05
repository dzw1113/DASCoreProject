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
import com.icip.framework.function.QueryCalDataFun;
import com.icip.framework.function.SavePayableResultFun;
import com.icip.framework.function.common.QueryDataByRedisFun;
import com.icip.framework.function.common.QueryFieldsByDataFun;
import com.icip.framework.spout.RedisPubSubSpout;
import com.icip.framework.topology.base.AbstractBaseTopo;

/** 
 * @Description: 小区应交款统计表(按住户汇总)
 * @author  
 * @date 2016年4月1日 下午5:49:03 
 * @update	
 */
public class PayableByHidTopo extends AbstractBaseTopo {

	public PayableByHidTopo(String topic, String spoutId, String topologyName) {
		super(topic, spoutId, topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topic = "vt_bill";

	// 消费者唯一标示
	public static final String spoutId = "vt_bill_hid_c"
			+ new Random().nextInt(10000);

	// topogy名称
	public static final String topologyName = "billDetailByHidTopo"
			+ new Random().nextInt(10);

	// false本地 true线上
	private static boolean flag = false;

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		JedisPoolConfig poolConfig = RedisClientPool.getStormRedisConfig();
		TridentTopology topology = new TridentTopology();
		RedisPubSubSpout spout = new RedisPubSubSpout(topic);
		Stream stream = topology.newStream(topologyName, spout).parallelismHint(1);

		// 按住户保存应交款结果
		stream
				.each(new Fields(STR), new QueryDataByRedisFun(poolConfig),
						new Fields(DASConstants.DATA)).parallelismHint(10).each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("plotId"), new Fields("plotId"))   
				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("hid"), new Fields("hid"))
				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("year"), new Fields("year"))
			    .each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("month"), new Fields("month"))
				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("roomNo"), new Fields("roomNo"))
				.each(new Fields(DASConstants.DATA),new QueryFieldsByDataFun("ownerName"), new Fields("ownerName"))	
				.each(new Fields(DASConstants.DATA), new QueryFieldsByDataFun("plotId","hid","year","month"), new Fields("totalKeyByHid"))
				.each(new Fields("totalKeyByHid"), new QueryCalDataFun(poolConfig,
						"cal_payableTotalByHid"),new Fields("payableTotalByHid"))
				.each(new Fields("totalKeyByHid"), new QueryCalDataFun(poolConfig,
						"cal_receivedTotalByHid"),new Fields("receivedTotalByHid"))
				.each(new Fields("totalKeyByHid"), new QueryCalDataFun(poolConfig,
						"cal_arrearsTotalByHid"),new Fields("arrearsTotalByHid"))
				.each(new Fields("plotId","hid","year","month","roomNo","ownerName","payableTotalByHid","receivedTotalByHid","arrearsTotalByHid"),
						new SavePayableResultFun(poolConfig,
						new String[]{"plotId","hid","year","month","roomNo","ownerName","payableAmount","receivedAmount","arrears"}),new Fields());
		
		return topology.build(); 
	}

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new PayableByHidTopo(topic, spoutId, topologyName).execute(flag);
	}

}
