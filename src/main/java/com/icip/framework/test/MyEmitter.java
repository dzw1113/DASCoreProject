package com.icip.framework.test;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata>{
	
	
	Map<Long, String>  dbMap = null;

	public MyEmitter(){}
	
	public MyEmitter(Map<Long, String> dbMap) {
		
		this.dbMap = dbMap;
	}


	/**
	 * 接收Coordinator事务tuple后，会进行batch tuple的发射，逐个发射batch的tuple
	 *   
	 * @param tx 
	 * 		必须以TransactionAttempt作为第一个field,含两个值：一个transaction id，一个attempt id。
	 * 		transaction id的作用就是我们上面介绍的对于每个batch中的tuple是唯一的
     * 		，而且不管这个batch    replay多少次都是一样的。attempt id是对于每个batch唯一的一个id， 但是对于同一个batch，
     * 		它replay之后的attempt id跟replay之前就不一样了，
	 */
	@Override
	public void emitBatch(TransactionAttempt tx, MyMata coordinatorMeta,
			BatchOutputCollector collector) {
		
		long beginPoint = coordinatorMeta.getBeginPoint();
		long num = coordinatorMeta.getNum();
		
		//每次发送数据源的量的多少，根据元数据决定
		for (long i = beginPoint; i < num+beginPoint; i++) {
			
			if (dbMap.get(i)==null) {
				continue;
			}
			collector.emit(new Values(tx,dbMap.get(i)));
		}
	}

	//清理之前事务的信息
	@Override
	public void cleanupBefore(BigInteger txid) {
		
		
	}

	@Override
	public void close() {
		
	}

}
