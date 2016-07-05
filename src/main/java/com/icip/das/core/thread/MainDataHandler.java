package com.icip.das.core.thread;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableRollingBean;
import com.icip.das.core.jdbc.ErrorRecordUtil;
import com.icip.das.core.jdbc.QueryDataUtil;
import com.icip.das.core.jdbc.RollingRecordUtil;
import com.icip.das.core.kafka.KafkaProducer;
import com.icip.das.core.redis.RedisClient;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;
import com.icip.das.core.validate.DataFilterChain;
import com.icip.das.util.TimeUtil;

/**
 * @Description: TODO
 * @author yzk
 * @date 2016年3月8日 上午10:12:01
 * @update
 */
@SuppressWarnings("unchecked")
public class MainDataHandler implements IDataHandler {

	private static final Logger logger = LoggerFactory
			.getLogger(MainDataHandler.class);

	@Override
	public void getDataResource(WorkerProxyObject proxy) {
		TableConfigBean config = (TableConfigBean) proxy.getConfig();
		String sql = proxy.getExcuteSql();
		List<Map<String, String>> data = null;
		try {
			data = QueryDataUtil.queryRemote(config, sql);
		} catch (Exception e) {
			ErrorRecordUtil.saveQueryErrorSql(config, sql);
			logger.error(e.toString(), e);
		}
		proxy.setData(data);
	}

	@Override
	public void handleData(WorkerProxyObject proxy) {
		TableConfigBean config = (TableConfigBean) proxy.getConfig();
		String tableName = config.getTableName();
		try {
			DataFilterChain.CHAIN.doHandle(proxy);
		} catch (Exception ex) {
			logger.error(tableName + "数据处理出错-------------->");
			logger.error(ex.toString(), ex);
		}
	}

	@Override
	public void doneWork(WorkerProxyObject proxy) {
		TableConfigBean config = (TableConfigBean) proxy.getConfig();
		String tableName = config.getTableName();

		TableRollingBean rollingBean = RollingBeanContainer
				.getRollingBean(tableName);
		if (null == rollingBean) {
			return;
		}

		List<Map<String, String>> params = (List<Map<String, String>>) proxy
				.getData();
		sendDataByRedis(tableName, params);

		updateDbConfig(proxy, config, rollingBean);
	}

	/**
	 * @Description: redis发布订阅
	 * @param @param tableName
	 * @param @param params
	 * @return void
	 * @throws
	 * @author
	 */
	public void sendDataByRedis(String tableName,
			List<Map<String, String>> params) {
		List<String> keys = RedisClient.setDataToRedis(tableName, params);
		RedisClient.setPublish(tableName, keys);
	}

	/**
	 * @Description: kafka形式
	 * @param @param tableName
	 * @param @param proxy
	 * @param @param config
	 * @param @param rollingBean
	 * @return void
	 * @throws
	 * @author
	 */
	public void sendDataByKafka(String tableName, WorkerProxyObject proxy,
			TableConfigBean config, TableRollingBean rollingBean) {
		List<Map<String, String>> params = (List<Map<String, String>>) proxy
				.getData();
		for (Map<String, String> param : params) {
			String key = param.get("rollingUniqueCondition");
			param.remove("rollingUniqueCondition");

			String redisKey = tableName + ":" + key;
			try {
				RedisClient.setDataToRedis(redisKey, param);
			} catch (Exception ex) {
				ErrorRecordUtil.saveRedisError(config, redisKey, param);
				updateDbConfig(proxy, config, rollingBean);
				logger.error(ex.toString(), ex);
				return;
			}

			try {
				String handleFlag = config.getHandleFlag();
				if (!StringUtils.isBlank(handleFlag) && "1".equals(handleFlag)) {
					List<String> list = new ArrayList<String>();
					list.add(redisKey);
					KafkaProducer producer = new KafkaProducer(tableName, list);
					producer.send();
				}
			} catch (Exception e) {
				ErrorRecordUtil.saveKafkaError(tableName, redisKey);
				logger.error(e.toString(), e);
			}
		}
	}

	public Object clone() {
		return null;
	}

	private void updateDbConfig(WorkerProxyObject proxy,
			TableConfigBean config, TableRollingBean rollingBean) {
		String tableName = config.getTableName();
		if (proxy.isFinish()) {
			// String updateTime =
			// TimeUtil.formatDate(rollingBean.getLastRollingTime());
			String updateTime = TimeUtil.formatDate(new Date(System
					.currentTimeMillis()));// FIXME 当前时间
			RollingRecordUtil.updateRollingTime(config);// 更新数据库最新轮询时间
			// ResultColumnRecord.updateRelation(tableName, params.get(0));
			logger.info(tableName
					+ "--------已执行至最新的更新时间为,子线程内可能存在未提交数据,检查数据库进程--------->"
					+ updateTime);
		}
	}

}
