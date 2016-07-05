package com.icip.framework.spout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.constans.DASConstants;

public class RedisSpout implements IBatchSpout {

	private static final long serialVersionUID = 5838607224585410984L;
	String key;
	int maxBatchSize = 10000;
	List<Object> outputs;
	HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

	public static final JedisPool jedisPool = RedisClientPool.getPool();
	
	public RedisSpout(String key) {
		this.key = key;
	}

	int index = 0;
	boolean cycle = false;

	public void setCycle(boolean cycle) {
		this.cycle = cycle;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		Jedis jedis = jedisPool.getResource();
		Set<String> set = jedis.keys(key);
		outputs = Arrays.asList(set.toArray());
		System.err.println(key+"长度------------------>"+set.size());
		index = 0;
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batch = this.batches.get(batchId);
		if (batch == null) {
			batch = new ArrayList<List<Object>>();
			if (index >= outputs.size() && cycle) {
				index = 0;
			}
			for (int i = 0; index < outputs.size() && i < maxBatchSize; index++, i++) {
				batch.add(new Values(outputs.get(index)));
			}
			this.batches.put(batchId, batch);
		}
		for (List<Object> list : batch) {
			collector.emit(list);
			Utils.sleep(10);
		}
	}

	@Override
	public void ack(long batchId) {
		this.batches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(DASConstants.STR);
	}

}
