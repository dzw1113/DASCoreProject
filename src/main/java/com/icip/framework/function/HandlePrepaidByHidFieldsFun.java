package com.icip.framework.function;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.function.common.AbstractRedisConnFun;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月14日 上午11:22:58 
 * @update	
 */
public class HandlePrepaidByHidFieldsFun extends AbstractRedisConnFun {

	private static final long serialVersionUID = -8585173489601504387L;

	public HandlePrepaidByHidFieldsFun(JedisPoolConfig poolConfig) {
		super(poolConfig);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Map<String,String> orginalData = (Map<String, String>) tuple.get(0);

		String cid = orginalData.get("cid");
		String hid = orginalData.get("hid");
		String year = orginalData.get("year");
		String month = orginalData.get("month");
		String roomNo = orginalData.get("roomNo");
		String ownerName = orginalData.get("pname");
		String chargeNo = orginalData.get("chargeNo");
		String hidChargeKey = cid + ":" + hid + ":" + chargeNo + ":" + year + ":" + month;
		String hidKey = cid + ":" + hid + ":"  + year + ":" + month;

		//预收金额合计
		String totalPrepaidAmount = getValue("cal_prepaidTotalByHid:" + hidKey);
		//取抵充金额合计
		String totalOffsetAmount = getValue("cal_offsetTotalByHid:" + hidKey);
		//取应交款合计
		String totalPayableAmount = getValue("cal_payableTotalByHid:" + hidKey);

		//根据费项取预收
		String chargeAmount = getValue("cal_prepaidTotalByHidAndChargeNo:" + hidChargeKey);
		//根据费项取应收
		String payableAmount = getValue("cal_payableTotalByHidAndChargeNo:" + hidChargeKey);
		//根据费项取冲销
		String offsetAmount = getValue("cal_offsetTotalByHidAndChargeNo:" + hidChargeKey);
		
		//取预缴表余额总和(期末余额)
		String prepaidInfoKey = "vt_prepaid_info:" + cid + ":" + hid + ":*:" + year + ":" + month; 
		String currentAmount = getPrepaidInfoValue(prepaidInfoKey);
		
		//上期余额
		String previousBlance = getPreviousBlance(currentAmount,offsetAmount);
		
		Map<String,String> resultParam = new HashMap<String,String>();
		resultParam.put("cid", cid);
		resultParam.put("hid", hid);
		resultParam.put("year", year);
		resultParam.put("month", month);
		resultParam.put("roomNo", roomNo);
		resultParam.put("ownerName", ownerName);
		resultParam.put("chargeNo", chargeNo);
		resultParam.put("totalPrepaidAmount", totalPrepaidAmount);
		resultParam.put("totalOffsetAmount", totalOffsetAmount);
		resultParam.put("totalPayableAmount", totalPayableAmount);

		resultParam.put("chargeAmount", chargeAmount);
		resultParam.put("payableAmount", payableAmount);
		resultParam.put("offsetAmount", offsetAmount);
		
		resultParam.put("currentAmount", currentAmount);
		resultParam.put("previousBlance", previousBlance);
		
		String resultKey = "rt_prepaid_hid" + ":" + hidChargeKey;
		saveResult(resultKey,resultParam);
	}

	private String getValue(String key){
		String redisVal = null;
		Jedis jedis = null;
		try {
			jedis = getJedis();
			redisVal = jedis.get( key);
			if(StringUtils.isBlank(redisVal)){
				//donothing
			}else{
				//截掉标识位
				redisVal = redisVal.split(",")[1].split("]")[0];
			}
		}catch(Exception ex){
			System.out.println(ex);
		} finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
		if(StringUtils.isBlank(redisVal)){
			return "0";
		}
		return redisVal;
	}
	
	//查询预缴信息表中单户所有费项总和
	private String getPrepaidInfoValue(String fieldKey){
		Float amount = Float.valueOf("0");
		Jedis redisPipeClient = null;
		try{
			redisPipeClient = RedisClientPool.getResource();
			Pipeline pipe = redisPipeClient.pipelined();
			Set<String> keys = redisPipeClient.keys(fieldKey);

			Map<String,Response<Map<String, String>>> responses = new HashMap<String,Response<Map<String, String>>>(keys.size()); 
			for(String key : keys) { 
				responses.put(key, pipe.hgetAll(key)); 
			} 
			pipe.sync(); 
			for(String k : responses.keySet()) {
				String temp = responses.get(k).get().get("prepaidAmount");
				if(!StringUtils.isBlank(temp)){
					amount = amount + Float.valueOf(temp);
				}
			} 
		}catch(Exception ex){
			System.out.println(ex);
		}finally{
			if(null != redisPipeClient){
				redisPipeClient.close();
			}
		}
		return amount.toString();
	}
	
	//	计算本期上期余额 预缴表余额+抵充
	private String getPreviousBlance(String currentAmountStr,String offsetAmountStr){
		float currentAmount = Float.valueOf(currentAmountStr);
		float offsetAmount = Float.valueOf(offsetAmountStr);
		Float temp = currentAmount + offsetAmount;
		return temp.toString();
	}
	
	private void saveResult(String key,Map<String,String> param){
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.hmset(key, param);
		} catch(Exception e){
			System.out.println(e);
		}finally {
			if (jedis != null) {
				returnJedis(jedis);
			}
		}
	}
	
}
