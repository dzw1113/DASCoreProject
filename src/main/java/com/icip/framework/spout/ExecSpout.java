package com.icip.framework.spout;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.framework.constans.DASConstants;

public class ExecSpout {

	public static StormTopology buildPPTopology(LocalDRPC a_drpc) {

		RedisAckPubSubSpout spout = new RedisAckPubSubSpout("bs_bill_details");
		TridentTopology topology = new TridentTopology();

		topology.newStream("tridentSp", spout).parallelismHint(10).each(new Fields(DASConstants.STR), new TestErrorFun(),new Fields());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordCounter", conf, buildPPTopology(drpc));
			for (int i = 0; i < 100; i++) {
				Thread.sleep(1000);
			}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildPPTopology(null));
		}
	}
}
