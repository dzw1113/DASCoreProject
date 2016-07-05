package com.icip.framework.spout;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableRollingBean;
import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.jdbc.QueryDataUtil;
import com.icip.das.core.jdbc.RollingRecordUtil;
import com.icip.das.util.TimeUtil;
import com.icip.framework.constans.DASConstants;

public class MysqlSpout implements IRichSpout {

	private static final long serialVersionUID = -6238368186011319683L;

	boolean isDistributed;
	static int limitSize = 10000;
	SpoutOutputCollector collector;

	public MysqlSpout() {
		this(true);
	}

	public MysqlSpout(boolean isDistributed) {
		this.isDistributed = isDistributed;
	}

	public boolean isDistributed() {
		return this.isDistributed;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		DataSourceLoader.DATASOURCE_INSTANCE.init();
	}

	public void close() {

	}

	public void nextTuple() {
		System.err.println("开始啦？？？？？？？？？？？？？？？？？？？？？");

		for (Map.Entry<String, TableConfigBean> entry : DataInitLoader
				.loadTbConfig().entrySet()) {
			TableConfigBean config = entry.getValue();
			String tableName = config.getTableName();
			TableRollingBean rollingConfig = RollingBeanContainer
					.getRollingBean(tableName);
			if (null == rollingConfig) {
				continue;
			}
			Date nextRollingTime = rollingConfig.getNextRollingTime();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Date currentTime = new Date();
			// System.err.println(sdf.format(nextRollingTime));
			// System.err.println(sdf.format(currentTime));
			// long time = nextRollingTime.getTime() - currentTime.getTime() ;
			// if (time / 1000 * 60 < 5) {
			// continue;
			// }
			String sql = "";//QueryDataUtil.getQuerySql(config,
//					TimeUtil.formatDate(nextRollingTime),
//					TimeUtil.formatDate(currentTime));

			long total = QueryDataUtil.getTotalNo(config.getSourceName(),
					tableName, sql);
			if (total == 0) {
				this.collector.emit(new Values(null, null));
			} else {
				long size = total % limitSize > 0 ? total / limitSize + 1
						: total % limitSize;
				size = size == 0 ? 1 : size;
				for (int i = 0; i < size; i++) {
					String newSql = sql;
					int start = (i == 0 ? 0 : i * limitSize);
					newSql = newSql + " limit " + start + "," + (limitSize);
					final Values row = new Values(newSql, config);
					System.err
							.println(tableName + ":" + start);

					this.collector.emit(row);
				}
				rollingConfig.setLastRollingTime(currentTime);
				RollingRecordUtil.updateRollingTime(config);
			}

		}
		try {
			Thread.sleep(300 * 1000);// 300秒轮询
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {

	}

	public void fail(Object msgId) {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(DASConstants.SQL,
				DASConstants.TABLECONFIGBEAN));
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
