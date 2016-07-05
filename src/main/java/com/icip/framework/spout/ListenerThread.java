package com.icip.framework.spout;

import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import com.icip.das.core.redis.RedisClientPool;

public class ListenerThread extends Thread {
	private static LinkedBlockingQueue<String> queue;
	private static JedisPool pool = RedisClientPool.getPool();
	private static String pattern;

	private static volatile ListenerThread instance = null;

	private ListenerThread() {
	}
	
	public static ListenerThread getInstance(LinkedBlockingQueue<String> queue,
			String pattern) {
		if (instance == null) {
			synchronized (ListenerThread.class) {
				if (instance == null) {
					instance = new ListenerThread(queue, pool, pattern);
					instance.start();
				}
			}
		}
		return instance;
	}

	public static ListenerThread getInstance(LinkedBlockingQueue<String> queue,
			JedisPool pool, String pattern) {
		if (instance == null) {
			synchronized (ListenerThread.class) {
				if (instance == null) {
					instance = new ListenerThread(queue, pool, pattern);
					instance.start();
				}
			}
		}
		return instance;
	}
	

	private ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool,
			String pattern) {
		this.queue = queue;
		this.pool = pool;
		this.pattern = pattern;
	}

	public void run() {
		// JedisPubSub 对象去Redis中间去订阅一个数据的通道
		JedisPubSub listener = new JedisPubSub() {

			@Override
			public void onMessage(String channel, String message) {
				queue.offer(message);
				System.err.println(message);
			}

			@Override
			public void onPMessage(String pattern, String channel,
					String message) {
				queue.offer(message);
			}

			@Override
			public void onPSubscribe(String channel, int subscribedChannels) {
			}

			@Override
			public void onPUnsubscribe(String channel, int subscribedChannels) {
			}

			@Override
			public void onSubscribe(String channel, int subscribedChannels) {
			}

			@Override
			public void onUnsubscribe(String channel, int subscribedChannels) {
			}
		};

		// 从池中取得对象
		Jedis jedis = pool.getResource();
		try {

			/**
			 ** listener 一个监听的线程 pattern 模式
			 **/
			jedis.psubscribe(listener, pattern);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("jedis报错！");
		} finally {
			try {
				jedis.close();
			} catch (Exception e) {
				System.err.println("jedis关闭链接报错！");
			}
		}
	}
};
