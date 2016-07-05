package com.icip.framework.topology.main;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.LocalCluster;

import com.icip.framework.topology.calc.TotalArreasAmountTopo;
import com.icip.framework.topology.calc.TotalBillAmountTopo;
import com.icip.framework.topology.calc.TotalBillArreasAmountTopo;
import com.icip.framework.topology.calc.TotalBillBussAmountTopo;
import com.icip.framework.topology.calc.TotalIncomeAmountTopo;
import com.icip.framework.topology.calc.TotalLateFeeTopo;
import com.icip.framework.topology.calc.TotalOffsetTopo;
import com.icip.framework.topology.calc.TotalPrepaidAmountTopo;

public class SubmitTopo {

	private static boolean flag = false;

	public static final Logger log = LoggerFactory.getLogger(SubmitTopo.class);

	public static void main(String[] args) {
		LocalCluster cluster = null;
		if (!flag) {
			cluster = new LocalCluster();
		}
		try {
			// 历史欠费
			String taa_topologyName = "Calc_TotalArreasAmount_TOPO"
					+ new Random().nextInt(10000);
			new TotalArreasAmountTopo(taa_topologyName).execute(flag, cluster);
			log.info("提交历史欠费-----------------end");

			// 账单中应交款,已交款，尚欠款
			String ba_topologyName = "Calc_TotalBillAmount"
					+ new Random().nextInt(10000);
			new TotalBillAmountTopo(ba_topologyName).execute(flag, cluster);
			log.info("提交账单中应交款,已交款，尚欠款-----------------end");

			// 账单欠费
			String baa_topologyName = "Calc_TotalBillArreasAmount_TOPO"
					+ new Random().nextInt(10000);
			new TotalBillArreasAmountTopo(baa_topologyName).execute(flag,
					cluster);
			log.info("提交账单欠费-----------------end");

			// 本月账单收款总金额
			String tba_topologyName = "Calc_TotalBillBussAmount_TOPO"
					+ new Random().nextInt(10000);
			new TotalBillBussAmountTopo(tba_topologyName)
					.execute(flag, cluster);
			log.info("提交本月账单收款总金额-----------------end");

			// 本月补收(本月收以前月份的账单金额)
			String ia_topologyName = "Calc_TotalIncomeAmount_TOPO"
					+ new Random().nextInt(10000);
			new TotalIncomeAmountTopo(ia_topologyName).execute(flag, cluster);
			log.info("提交本月补收-----------------end");

			// 滞纳金
			String lf_topologyName = "Calc_TotalLateFee_TOPO"
					+ new Random().nextInt(10000);
			new TotalLateFeeTopo(lf_topologyName).execute(flag, cluster);
			log.info("提交滞纳金-----------------end");

			// 冲销总额统计
			String of_topologyName = "Calc_TotalOffset"
					+ new Random().nextInt(10000);
			new TotalOffsetTopo(of_topologyName).execute(flag, cluster);
			log.info("提交冲销总额-----------------end");

			// 计算预收总和
			String pa_topologyName = "Calc_TotalPrepaidAmount"
					+ new Random().nextInt(10000);
			new TotalPrepaidAmountTopo(pa_topologyName).execute(flag, cluster);
			log.info("提交预收总和-----------------end");
			
			
			Thread.sleep(20000000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		cluster.shutdown();
		System.exit(0);
	}
}
