package com.icip.das.core.thread;


import java.util.Timer;
import java.util.TimerTask;

/**
* 定时器
*
*/
public class CoreTimer {
	
	private static Timer timer;

	private CoreTimer() {
	}

	/**
	 * 启动定时器.<br>
	 * @param task
	 * @param delay
	 * @param period
	 */
	public static synchronized void schedule(TimerTask task, long delay, long period) {
		if (null == timer) {
			timer = new Timer(true);
		}
		timer.schedule(task, delay, period);
	}

	/**
	 * 取消定时器.<br>
	 * @param task
	 */
	public static synchronized void cancel(TimerTask task) {
		if (task != null) {
			task.cancel();
		}
		if (timer != null) {
			timer.cancel();
		}
		timer = null;
	}
}

