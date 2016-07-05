package com.icip.framework.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import redis.clients.jedis.JedisPool;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.icip.das.core.redis.RedisClientPool;
import com.icip.framework.constans.DASConstants;

/**
 * @Description: 该spout带ack机制
 * @author
 * @date 2016年4月26日 下午2:47:01
 * @update
 */
public class RedisAckPubSubBaseSpout extends BaseRichSpout {

	static final long serialVersionUID = 737015318988609460L;
	static Logger LOG = Logger.getLogger(RedisAckPubSubBaseSpout.class);

	HashMap<Long, String> batches = new HashMap<Long, String>();

	SpoutOutputCollector _collector;
	final String pattern;
	LinkedBlockingQueue<String> queue;
	JedisPool pool;
	Long inx = 0l;

	public RedisAckPubSubBaseSpout(String pattern) {
		this.pattern = pattern;
	}

	public RedisAckPubSubBaseSpout(String pattern, JedisPool pool) {
		this.pattern = pattern;
		this.pool = pool;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		// 初始化队列，pool，和监控的线程
		queue = new LinkedBlockingQueue<String>();
		if (pool == null) {
			pool = RedisClientPool.getPool();
		}
		ListenerThread.getInstance(queue, pool, pattern);
	}

	public void close() {
		pool.destroy();
	}

	public void nextTuple() {
		// 从队列之中取值的一个过程，在这里判断为空以后，就开始不断的使用Storm发射数据
		String ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
			// continue;
		} else {
			Long batchId = inx++;
			_collector.emit(tuple(ret), batchId);
			this.batches.put(batchId, ret);
		}
	}

	public void ack(Object msgId) {
		this.batches.remove(msgId);
	}

	public void fail(Object msgId) {
		System.err.println(batches.get(msgId));
		queue.offer(batches.get(msgId));
		batches.remove(msgId);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(DASConstants.STR));
	}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.put("topology.spout.max.batch.size", 1);
		return ret;
	}
}
