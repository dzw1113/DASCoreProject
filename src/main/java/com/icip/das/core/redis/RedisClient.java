package com.icip.das.core.redis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import backtype.storm.utils.Utils;

import com.icip.das.core.data.TableConfigBean;

/**
 * 
 * @Description: redis客户端普遍操作
 * @author
 * @date 2016年3月22日 上午11:12:59
 * @update
 */
public class RedisClient implements Serializable {

	private static final long serialVersionUID = -979474123286063179L;

	private static org.slf4j.Logger LOG = LoggerFactory
			.getLogger(RedisClient.class);

	/**
	 * 保存数据 类型为 Map <一句话功能简述> <功能详细描述>
	 * 
	 * @param flag
	 * @param mapData
	 * @see [类、类#方法、类#成员]
	 */
	public static void setMapDataToRedis(String flag,
			Map<String, String> mapData) {
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			redisClient.hmset(flag, mapData);
		} catch (Exception e) {
			LOG.error("保存数据出错！", e);
		} finally {
			RedisClientPool.close(redisClient);
		}
	}

	/**
	 * 保存数据 类型为 key-value <一句话功能简述> <功能详细描述>
	 * 
	 * @param flag
	 * @param field
	 * @param value
	 * @see [类、类#方法、类#成员]
	 */
	public static void setDataToRedis(String flag, String field, String value) {
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			redisClient.hset(flag, field, value);
		} catch (Exception e) {
			LOG.error("保存数据出错！", e);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
	}

	/**
	 * 
	 * @Description: 保存数据
	 * @param @param key
	 * @param @param mapData
	 * @return void
	 * @throws
	 * @author
	 */
	public static void setDataToRedis(String key, Map<String, String> mapData) {
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			redisClient.hmset(key, mapData);
		} catch (Exception e) {
			throw e;
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
	}
	
	
	/**
	 * 
	 * @Description: 保存数据
	 * @param @param key
	 * @param @param mapData
	 * @return void
	 * @throws
	 * @author
	 */
	public static List<String> setDataToRedis(String tableName, List<Map<String, String>> list) {
		List<String> keys = new ArrayList<String>();
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			
			Pipeline pip = redisClient.pipelined();
			
			for(Map<String,String> param : list){
				String unique = param.get("rollingUniqueCondition");
				param.remove("rollingUniqueCondition");
				String redisKey = tableName + ":" + unique;
				pip.hmset(redisKey, param);
				keys.add(redisKey);
			}
			pip.sync();
			
		} catch (Exception e) {
			throw e;
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
		return keys;
	}
	
	/**
	 * 
	 * @Description: 发布数据（管道）
	 * @param @param key
	 * @param @param mapData
	 * @return void
	 * @throws
	 * @author
	 */
	public static void setPublish(String tableName,List<String> keys) {
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			Pipeline pip = redisClient.pipelined();
			for (String key : keys) {
				pip.publish(tableName, key);
			}
			pip.sync();
		} catch (Exception e) {
			throw e;
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
	}


	/**
	 * 获取Map数据 <一句话功能简述> <功能详细描述>
	 * 
	 * @param flag
	 * @return
	 * @see [类、类#方法、类#成员]
	 */
	public static Map<String, String> getMapData(String flag) {
		Map<String, String> dataMap = null;

		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			dataMap = redisClient.hgetAll(flag);
		} catch (Exception e) {
			LOG.error("获取Map数据出错！", e);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
		return dataMap;
	}

	/**
	 * 
	 * @Description: 删除key
	 * @param @param key
	 * @param @return
	 * @return long
	 * @throws
	 * @author
	 */
	public static long deleteData(String key) {
		long result = 0;
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			result = redisClient.del(key);
		} catch (Exception e) {
			LOG.error("删除key出错！", e);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
		return result;
	}

	public static long deleteDatas(String... flags) {
		long result = 0;
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			result = redisClient.del(flags);
		} catch (Exception e) {
			// 销毁对象
			RedisClientPool.close(redisClient);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}

		return result;
	}

	/**
	 * 
	 * @Description: 保存数据(管道)
	 * @param @param key
	 * @param @param mapData
	 * @return void
	 * @throws
	 * @author
	 */
	public static List<String> setDataToRedis(List<Map<String, String>> data,
			TableConfigBean conf) {
		List<String> list = new ArrayList<String>();
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			Pipeline p = redisClient.pipelined();
			for (int i = 0; i < data.size(); i++) {
				Map<String, String> map = data.get(i);
				p.hmset(conf.getTableName() + ":"
						+ map.get("rollingUniqueCondition"), data.get(i));
				list.add(conf.getTableName() + ":"
						+ map.get("rollingUniqueCondition"));
			}
			p.sync();
		} catch (Exception e) {
			throw e;
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
		return list;
	}

	/**
	 * 根据key和字段获取数据 <一句话功能简述> <功能详细描述>
	 * 
	 * @param flag
	 * @param field
	 * @return
	 * @see [类、类#方法、类#成员]
	 */
	public static String getData(String flag, String field) {
		String data = null;
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			data = redisClient.hget(flag, field);
		} catch (Exception e) {
			LOG.error("根据key和字段获取数据出错！", e);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}
		return data;
	}

	/**
	 * 
	 * @Description: 从队列左边插入
	 * @param @param tableName
	 * @param @param pkVal
	 * @return void
	 * @throws
	 * @author
	 */
	public static void lpushKey(String tableName, String pkVal) {
		Jedis redisClient = null;
		try {
			redisClient = RedisClientPool.getResource();
			redisClient.lpush(tableName, pkVal);
		} catch (Exception e) {
			LOG.error("保存数据出错！", e);
		} finally {
			// 还原到连接池
			RedisClientPool.close(redisClient);
		}

	}
	
	// -------------------以下测试
	public static void main(String[] args) throws Exception {

		String host = "192.168.19.188";
		int port = 6379;
		int timeout = 60000;
		String password = "admin";
		final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();
		JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG, host, port,
				timeout, password);

		List<String> list = new ArrayList<String>();

		// list.add("vt_bill");
		// list.add("vt_house_charge");
		// list.add("vt_valuation");
		// list.add("bd_valuation_item");
		//
		// list.add("bd_charge_item");
		// list.add("bd_charge_config");
		// list.add("bd_community_base_info");
		// list.add("bd_company_info");
		// list.add("bd_house_base_info");
		// list.add("bd_proprietor_base_info");
		// list.add("icip_sys_para");
		// list.add("bs_business_journals");
		// list.add("bs_offset_jouranls");
		// list.add("bs_transpay_journals");
		// list.add("bs_trans_journals");
		// list.add("bd_house_charge");
		// list.add("bs_business_extend");
		// list.add("bs_prepaid_info");
		// list.add("bs_arrears_info");
		// list.add("bd_related_assets");
		// list.add("vt_house_charge");
		// list.add("VT_THIS_MONTH_COLLECTION");
		// list.add("payableTotalAmount");
//		list.add("rt_YearBillAmount");
		// list.add("cal_RealAmountByChargeNo");
//		 list.add("rt_MonthBillAmount");
//		 list.add("rt_prepaid_hid");
//		 list.add("rt_DayBillAmount");
//		 list.add("vt_arreas_info");
		 list.add("rt_bill_charge");
		for (int i = 0; i < list.size(); i++) {
			String tableName = list.get(i);
			Jedis redisPipeClient = null;
			try {
				redisPipeClient = RedisClientPool.getResource();
				Pipeline pipe = redisPipeClient.pipelined();
				Set<String> keys = redisPipeClient.keys(tableName + ":*");
				// Set<String> keys =
				// redisPipeClient.keys("payableTotalAmount");
				if (tableName.equals("rt_bill_charge")) {
					Map<String, Response<Map<String, String>>> responses = new HashMap<String, Response<Map<String, String>>>(
							keys.size());
					for (String key : keys) {
						responses.put(key, pipe.hgetAll(key));
					}
					pipe.sync();
					Jedis jd = jedisPool.getResource();
					Pipeline pipe188 = jd.pipelined();
					for (String k : responses.keySet()) {
//						System.err.println(k);
//						Map map = responses.get(k).get();
//						if("H0000048953".equals(map.get("houseId"))){
//							System.err.println("sss");
//						}
//						 System.err.println(k);
//						 System.out.println(responses.get(k).get().size()==9);
						pipe188.hmset(k, responses.get(k).get());
//						jd.hmset(k, responses.get(k).get());
//						jd.close();
					}
					pipe188.sync();
					
				}
				System.out.println("-------批量任务，redis中" + tableName + "表数据，共"
						+ keys.size() + "条");
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				if (null != redisPipeClient) {
					redisPipeClient.close();
				}
			}
		}
	}

	public void testList() {
		Jedis redis = RedisClientPool.getResource();
		// hset key field value将哈希表key中的域field的值设为value。
		redis.hset("table", "field1", "value1");
		redis.hset("table", "field2", "value2");
		redis.hset("table", "field3", "value3");
		// 返回哈希表key中，一个或多个给定域的值。
		List<String> list = redis.hmget("table", "field1", "field2", "field3");
		for (String tmp : list) {
			System.out.println(tmp);
		}
	}

	public static void testMap() {
		// 同时将多个field - value(域-值)对设置到哈希表key中。
		Map<String, String> map = new ConcurrentHashMap<String, String>();
		for (int i = 0; i < 10000; i++) {
			map.put("field" + i, "value" + i);
		}
		if (null != getData("table", "field1")) {
			deleteData("table");
		}
		// 得到map下面的username的值
		Map<String, String> maps = getMapData("table");
		System.out.println(maps.size());

		setMapDataToRedis("table", map);

		// HGETALL key返回哈希表key中，所有的域和值。
		maps = getMapData("table");

		System.out.println(maps.size());
	}

}
