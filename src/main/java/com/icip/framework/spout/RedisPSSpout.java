package com.icip.framework.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;

public class RedisPSSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3680533991353753754L;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
	}

	@Override
	public void nextTuple() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}
	
	

}
