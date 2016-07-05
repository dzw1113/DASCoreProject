package com.icip.framework.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.JedisPool;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.constans.DASConstants;

public class RedisPubSubSpout implements IBatchSpout {

	private static final long serialVersionUID = -1619055458919373532L;
	final String pattern;
	LinkedBlockingQueue<String> queue;
	JedisPool pool;

	public RedisPubSubSpout(String pattern) {
		this.pattern = pattern;
	}

	public RedisPubSubSpout(String pattern, JedisPool pool) {
		this.pattern = pattern;
		this.pool = pool;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		// 初始化队列，pool，和监控的线程
		queue = new LinkedBlockingQueue<String>();
		if (pool == null) {
			pool = RedisClientPool.getPool();
		}

		ListenerThread.getInstance(queue, pool, pattern);
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// 从队列之中取值的一个过程，在这里判断为空以后，就开始不断的使用Storm发射数据
		for (int i = 0; i < DASConstants.batchSize; i++) {
			String ret = queue.poll();
			if (ret == null) {
				Utils.sleep(50);
				continue;
			} else {
				collector.emit(tuple(ret));
			}
		}

	}

	@Override
	public void ack(long batchId) {
	}

	@Override
	public void close() {
		pool.destroy();
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
