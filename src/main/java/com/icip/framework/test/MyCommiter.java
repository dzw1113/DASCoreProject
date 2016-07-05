package com.icip.framework.test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Commiting阶段，每个batch都会进行顺序提交，即事务一提交完成才会提交事务二
 *  
 *
 */
public class MyCommiter extends BaseTransactionalBolt implements ICommitter {
	
	public static Map<String,DBValue> dbMap = new HashMap<String, MyCommiter.DBValue>();

	private static final long serialVersionUID = 1L;
	
	public static final String GLOBAL_KEY = "GLOBAL_KEY";
	
	long sum = 0;
	
	TransactionAttempt id;
	
	BatchOutputCollector collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		this.id = id;
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		
		TransactionAttempt tx = (TransactionAttempt) tuple.getValue(0);
		Long count = tuple.getLong(1);
		sum += count;
	}

	
	//提交事务
	@Override
	public void finishBatch() {
	
		//更新数据库
		DBValue value = dbMap.get("GLOBAL_KEY");
		DBValue newvalue ;
		
		//第一次写入或者写入最新的数据
		if (value == null || !value.txid.equals(id.getTransactionId())) {
			
			newvalue = new DBValue();
			newvalue.txid = id.getTransactionId();
	
			//第一次
			if (value == null) {
				newvalue.count = sum;
			}else{
				newvalue.count =value.count+sum;
			}
			dbMap.put(GLOBAL_KEY, newvalue);
		}else{
			newvalue = value;
		}
		
		System.out.println("total----------------------------->"+dbMap.get(GLOBAL_KEY).count);
		
		
		//发送结果到下一级   collector.emit(new Values());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(""));
	}

	 public static class DBValue{
		
		 BigInteger txid;
		 long  count =0;
	}
}
