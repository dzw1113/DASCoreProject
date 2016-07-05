package com.curtinhome.com.trident.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.curtinhome.trident.spout.TransactionalFixedBatchSpout;
import com.icip.framework.function.common.PrintResultFun;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Testing Transactional topologies in Trident. Borrowed heavily from the WordCount example in Storm
 *
 */
public class TransactionalWordCount {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
            	collector.emit(new Values(word));
            }
        }
    }

    public static TransactionalFixedBatchSpout makeTransactionalSpout() {
        List<List<Values>> data = new ArrayList<List<Values>>();
        List<Values> block1 = new ArrayList<Values>();
        block1.add(new Values(1, "500", "ninety nine"));
        block1.add(new Values(2, "300", "ninety nine nine zero"));
        block1.add(new Values(3, "300", "ninety"));
        data.add(block1);

        return new TransactionalFixedBatchSpout(new Fields("code", "source", "sentence"), 3, data);
    }

    public static StormTopology buildPPTopology(LocalDRPC a_drpc) {
        //
           TransactionalFixedBatchSpout spout =  makeTransactionalSpout();
        TridentTopology topology = new TridentTopology();
        
        topology.newStream("PPSpout990", spout)
              .each(new Fields("sentence"), new Split(), new Fields("word"))
              .each(new Fields("source","word"), new PrintResultFun(),new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
//        conf.setDebug(true);
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
