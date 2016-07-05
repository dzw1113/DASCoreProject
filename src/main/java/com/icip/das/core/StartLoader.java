package com.icip.das.core;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.kafka.KafkaTopicInit;
import com.icip.das.core.redis.RedisClientPool;
import com.icip.das.core.task.ReloadTask;
import com.icip.das.core.thread.CoreTimer;
import com.icip.das.core.thread.DispatcherRollingTask;
import com.icip.das.core.thread.ShutdownThread;

/**
 * @Description: TODO
 * @author yzk
 * @date 2016年3月9日 下午3:33:01
 * @update
 */
public class StartLoader {

	public static Date addDay(Date date, int num) {
		Calendar startDT = Calendar.getInstance();
		startDT.setTime(date);
		startDT.add(Calendar.DAY_OF_MONTH, num);
		return startDT.getTime();
	}

	public static void main(String[] args) {

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 1); // 凌晨1点
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		Date date = calendar.getTime(); // 第一次执行定时任务的时间
		// 如果第一次执行定时任务的时间 小于当前的时间
		// 此时要在 第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
		if (date.before(new Date())) {
			date = addDay(date, 1);
		}

		LoadDataTimer ldt = new LoadDataTimer();
		Timer upTime = new Timer();
		
		Timer time = new Timer();
		// 立即一次
		upTime.schedule(ldt, 0);
		//到了晚上1点在执行
//		time.schedule(new LoadDataTimer(), date, 1000 * 60 * 60 * 24);
	}
}

class LoadDataTimer extends TimerTask {

	private static final Logger logger = LoggerFactory
			.getLogger(LoadDataTimer.class);

	public void run() {
		try {
			System.err.println("------------>轮询开始" + new Date().getTime());
			loadData();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void loadData() {
		
		// 初始化数据源配置
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		DataInitLoader.getTableConfig();
		RedisClientPool.getInstance();
		// 初始化kafka
		// KafkaProducerFactory.getProducer();
		// 初始化kafka topic
//		KafkaTopicInit.initTopic();

		Jedis jedis = RedisClientPool.getResource();
		jedis.flushAll();
		if (!DataServer.IS_RUNNING)
			DataServer.SERVER_INSTANCE.startServer();
		
		DispatcherRollingTask.dispach();

		// 启动轮询检查线程
//		if (!RollingMonitorTread.IS_RUNNING)
//			new RollingMonitorTread().start();

		CoreTimer.schedule(new ReloadTask(), 2 * 60 * 1000, 1 * 60 * 1000);
		// CoreTimer.schedule(new BatcHTASK(), 1 * 60 * 1000, 1 * 60 * 1000);
		// CORETIMER.SCHEDULE(NEW ERRORRetryTask(), 5 * 60 * 1000, 5 * 60 *
		// 1000);
		Runtime.getRuntime().addShutdownHook(new ShutdownThread());
	}

}
