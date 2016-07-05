package com.icip.framework.spout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import com.icip.framework.constans.DASConstants;

/**
 * @Description: 该spout带ack机制
 * @author
 * @date 2016年6月2日 下午4:44:24
 * @update
 */
public class RedisAckPubSubSpout implements ITridentSpout<String> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(RedisAckPubSubSpout.class);

	private String pattern;
	private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();

	private static HashMap<Long, String> batches = new HashMap<Long, String>();

	// Map<Long, String> batches = MapManager.getInstance();
	private static volatile AtomicLong atomic = new AtomicLong(0);
	
	private static volatile AtomicLong num = new AtomicLong(0);

	public RedisAckPubSubSpout(String pattern) {
		this.pattern = pattern;
		ListenerThread.getInstance(queue, pattern);
	}

	class RedisEmitter implements Emitter<String>, Serializable {

		private static final long serialVersionUID = -7873458746129116829L;

		@Override
		public void emitBatch(TransactionAttempt tx, String coordinatorMeta,
				TridentCollector collector) {
//			if (batches.size() > 0) {
//				String val = batches.get(atomic.get());
//				System.err.println(atomic+"   "+atomic.get() + "   " + val + "   " + tx);
//				if (val != null) {
//					queue.offer(val);
//					batches.remove(atomic.get());
//				}
//			}
//			List<Object> events = new ArrayList<Object>();
//			String ret = queue.poll();
//			if (ret != null) {
//				long id = atomic.incrementAndGet();
//				batches.put(id, ret);
//				events.add(ret);
//				collector.emit(events);
//			}
			long c = num.incrementAndGet();
			System.err.println(tx.getId()+"   "+ c);
			batches.put(tx.getTransactionId(), c+"dzw");

		}

		@Override
		public void success(TransactionAttempt tx) {
			batches.remove(atomic.get());
		}

		@Override
		public void close() {
		}

	}

	class RedisBatchCoordinator implements BatchCoordinator<String>,
			Serializable {
		private static final long serialVersionUID = -5452642039060330685L;

		@Override
		public String initializeTransaction(long txid, String prevMetadata,
				String currMetadata) {
			return null;
		}

		@Override
		public void success(long txid) {
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void close() {
		}

	}

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<String> getCoordinator(
			String txStateId, Map conf, TopologyContext context) {
		return new RedisBatchCoordinator();
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<String> getEmitter(
			String txStateId, Map conf, TopologyContext context) {
		return new RedisEmitter();
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

class MapManager {
	private static volatile Map<String, Object> instance = null;

	private MapManager() {
	}

	public static Map<String, Object> getMap() {
		if (instance == null) {
			synchronized (MapManager.class) {
				if (instance == null) {
					instance = new HashMap<String, Object>();
				}
			}
		}
		return instance;
	}

	// 用于保存缓存
	public synchronized static void put(String key, Object value) {
		instance.put(key, value);
	}

	// 用于得到缓存
	public synchronized static Object get(String key) {
		return instance.get(key);
	}
}
