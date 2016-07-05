package com.icip.framework.function;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: 查询费项单位
 * @author  
 * @date 2016年3月29日 上午11:00:23 
 * @update	
 */
public class QueryChargeUnitFun extends AbstractRedisConnFun  {

	private static final long serialVersionUID = 7966578941888674109L;

	public QueryChargeUnitFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String role = tuple.getString(0);
		if(StringUtils.isBlank(role)){
			collector.emit(new Values(""));
			return;
		}
		
		StringBuilder unit = new StringBuilder();
		//截取
		Pattern p = Pattern.compile("\\{.*?\\}");
		Matcher m = p.matcher(role);
		while (m.find()) {
			String param = m.group().replaceAll("\\{\\}", "");
			param = param.substring(1, param.lastIndexOf("}"));
			
			if(!"price".equals(param)){
				String key = "bd_valuation_item" + ":" + param;
				unit.append(getValue(key));
			}
		}
		collector.emit(new Values(unit.toString()));
	}

	private String getValue(String key){
		Jedis jedis = null;
		try {
			jedis = getJedis();
			List<String> redisVals = jedis.hmget(key, "unit");
			return redisVals.get(0);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}
	
}
