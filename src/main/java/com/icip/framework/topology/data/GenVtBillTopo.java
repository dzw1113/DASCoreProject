package com.icip.framework.topology.data;

import scala.util.Random;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.icip.framework.constans.DASConstants;
import com.icip.framework.function.data.QueryFieldsBySTRFun;
import com.icip.framework.function.data.QueryFieldsSTRFun;
import com.icip.framework.function.data.SaveKafkaFun;
import com.icip.framework.topology.base.AbstractBaseTopo;

public class GenVtBillTopo extends AbstractBaseTopo {

	public GenVtBillTopo(String topologyName) {
		super(topologyName);
	}

	private static final long serialVersionUID = 6641399625533369229L;

	public static final String topologyName = "Calc_TotalLateFee_TOPO"
			+ new Random().nextInt(10000);
	static boolean flag = false;

	public static void main(String[] args) throws Exception {
		if (args != null && args.length > 0) {
			flag = true;
		}
		new GenVtBillTopo(topologyName).execute(flag);
	}

	@Override
	public StormTopology buildTopology(LocalDRPC drpc) throws Exception {

		TridentTopology topology = new TridentTopology();
		TransactionalTridentKafkaSpout pSpout = getSpout("bs_pay_bills");
		TransactionalTridentKafkaSpout bSpout = getSpout("bs_bill_details");
		Stream ptream = topology.newStream(topologyName, pSpout);
		Stream btream = topology.newStream(topologyName + 1, bSpout);
		// NEW
		Stream bnStream = ptream.each(new Fields(STR), new QueryFieldsBySTRFun(
				"billNo"), new Fields("bBillNo"));
		Stream pnStream = btream.each(new Fields(STR), new QueryFieldsSTRFun(
				"billNo"), new Fields("pBillNo", "bSTR"));

		Stream stream = topology.join(bnStream, new Fields("bBillNo"),
				pnStream, new Fields("pBillNo"), new Fields(STR, "bSTR",
						DASConstants.DATA));

//		stream.each(new Fields(STR, "bSTR"), new SaveKafkaFun(),new Fields());
//		stream.each(new Fields(STR, "bSTR", DASConstants.DATA),
//				new PrintResultFun(), new Fields());

		return topology.build();
	}
}