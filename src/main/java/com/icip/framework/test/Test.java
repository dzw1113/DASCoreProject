package com.icip.framework.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

public class Test {

	public static void main(String[] args) throws InterruptedException {
		TridentTopology tt = new TridentTopology();
		MyTxSpout sp = new MyTxSpout();
	}
}
