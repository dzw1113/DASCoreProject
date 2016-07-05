package com.icip.framework.test;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMata>{

	//batch中tuple的个数
	private static int BATCH_NUM = 10;
	
	/**
	 * 启动一个事务，生产元数据，定义事务开始的位置和数量
	 * @param  txid 事务id,默认从0开始
	 * @param  prevMetadata 上一个元数据
	 * 
	 */
	@Override
	public MyMata initializeTransaction(BigInteger txid, MyMata prevMetadata) {
		
		long beginPoint = 0;
		
		if (prevMetadata == null) {
			//第一个事务，程序刚开始
			beginPoint = 0;
			
		}else{

			beginPoint = prevMetadata.getBeginPoint() + prevMetadata.getNum();
		}
		
		MyMata myMata = new MyMata();
		myMata.setBeginPoint(beginPoint);
		myMata.setNum(BATCH_NUM); 
		
		System.err.println("启动一个事务： "+myMata.toString());
		
		return myMata;
	}

	
	/**
	 * 只有返回为true，开启一个事务进入processing阶段，发射一个事务性的tuple到batch emit流，Emitter以广播方式订阅Coordinator的batch emit流
	 */
	@Override
	public boolean isReady() {
		
		Utils.sleep(2000);
		return true;
	}

	@Override
	public void close() {
		
	}
}
