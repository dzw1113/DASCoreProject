package com.icip.framework.spout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.topology.FailedException;

public class TestErrorFun extends BaseFunction {

	private static final Logger LOG = LoggerFactory
			.getLogger(TestErrorFun.class);

	private static long counter = 0;

	private static final long serialVersionUID = -4505509553569927472L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
		// LOG.info(tuple.getValues()+":-------counter:"+counter++);
		if (tuple.getString(0).equals(
				"bs_bill_details:BLLD14584284304640116:BSBP14584284304580113")) {
			LOG.info(tuple.getValues() + ":-------counter:" + counter++);
			throw new FailedException();
			// tridentCollector.reportError(new FailedException());
		}
	}

	// private final AtomicLong atomic = new AtomicLong(0);

	static int x = 0, y = 0;
	static int a = 0, b = 0;

	public static void main(String[] args) throws InterruptedException {
		Thread one = new Thread(new Runnable() {
			public void run() {
				a = 1;
				x = b;
			}
		});

		Thread other = new Thread(new Runnable() {
			public void run() {
				b = 1;
				y = a;
			}
		});

		one.start();
		other.start();
		one.join();
		other.join();

		System.out.println("( " + x + "," + y + ")");

	}
}
