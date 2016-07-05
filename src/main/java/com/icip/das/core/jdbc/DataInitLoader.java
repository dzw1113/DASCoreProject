package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableRollingBean;
import com.icip.das.util.TimeUtil;

/**
 * @Description: TODO
 * @author yzk
 * @date 2016年3月7日 下午3:42:39
 * @update
 */
public class DataInitLoader {

	private static final Logger logger = LoggerFactory.getLogger(DataInitLoader.class);

	public static ConcurrentMap<String, TableConfigBean> TABLE_CONFIG = new ConcurrentHashMap<String, TableConfigBean>();

	private static final String DEFAULT_ROLLING_INTERVAL = "30";// 默认轮询间隔30s

	private static Object lock = new Object();
	
	public static Map<String, TableConfigBean> getTableConfig() {
		if (null == TABLE_CONFIG || TABLE_CONFIG.isEmpty()) {
			synchronized (lock) {
				if (null == TABLE_CONFIG || TABLE_CONFIG.isEmpty()) {
					init();
				}
			}
		}
		return TABLE_CONFIG;
	}

	private static void init() {
		//加载轮询配置rolling_config
		loadRollingConfig();
		//加载自定义sql配置rolling_config_custom
		loadRollingConfigCustomer();
	}

	private static void loadRollingConfigCustomer() {
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = connection.createStatement();
			String queryStr = "select a.* from rolling_config_custom a where flag = 1";
			rs = stmt.executeQuery(queryStr);

			while (rs.next()) {
				TableConfigBean config = new TableConfigBean();
				String tableName = rs.getString("visual_table_name");
				config.setTableName(tableName);
				config.setCustomerSql(rs.getString("query_sql"));
				config.setUniqueCondition(rs.getString("unique_condition"));
				config.setHandleFlag(rs.getString("logic_handle_flag"));
				config.setSourceName(rs.getString("query_schema"));
				String period = rs.getString("rolling_period");
				if (StringUtils.isBlank(period)) {
					period = DEFAULT_ROLLING_INTERVAL;
				}
				config.setRollingPeriod(period);
				TABLE_CONFIG.put(tableName, config);
				
				TableRollingBean rollingBean = new TableRollingBean();
				Date lastRollingTime = TimeUtil.formatString(rs.getString("last_rolling_time"));
				rollingBean.setLastRollingTime(lastRollingTime);
				rollingBean.setNextRollingTime(TimeUtil.getNextTime(
						Double.valueOf(period), 1, lastRollingTime));
				RollingBeanContainer.setRollingBean(tableName, rollingBean);
			}
		} catch (Exception e) {
			logger.error(e.toString(), e);
		} finally {
			if (rs != null) {// 关闭记录集
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			if (stmt != null) { // 关闭声明
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			DataSourceUtil.close();
		}
	}

	private static void loadRollingConfig(){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = connection.createStatement();
			String queryStr = "select a.* from rolling_config a where flag = 1";
			rs = stmt.executeQuery(queryStr);

			while (rs.next()) {
				TableConfigBean config = new TableConfigBean();
				String tableName = rs.getString("table_name");
				config.setTableName(tableName);
				config.setUniqueCondition(rs.getString("unique_condition"));
				config.setHandleFlag(rs.getString("logic_handle_flag"));
				String period = rs.getString("rolling_period");
				config.setSourceName(rs.getString("table_schema"));
				if (StringUtils.isBlank(period)) {
					period = DEFAULT_ROLLING_INTERVAL;
				}
				config.setRollingPeriod(period);
				TABLE_CONFIG.put(tableName, config);

				TableRollingBean rollingBean = new TableRollingBean();
				Date lastRollingTime = TimeUtil.formatString(rs.getString("last_rolling_time"));
				rollingBean.setLastRollingTime(lastRollingTime);
				rollingBean.setNextRollingTime(TimeUtil.getNextTime(
						Double.valueOf(period), 1, lastRollingTime));
				RollingBeanContainer.setRollingBean(tableName, rollingBean);
			}
		} catch (Exception e) {
			logger.error(e.toString(),e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			DataSourceUtil.close();
		}
	}
	
	
	public static Map<String, TableConfigBean> loadTbConfig(){
		Map<String, TableConfigBean> map = new HashMap<String, TableConfigBean>();
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;

		try {
			stmt = connection.createStatement();
			String queryStr = "select a.* from rolling_config a";
			rs = stmt.executeQuery(queryStr);

			while (rs.next()) {
				TableConfigBean config = new TableConfigBean();
				String tableName = rs.getString("table_name");
				config.setTableName(tableName);
				config.setUniqueCondition(rs.getString("unique_condition"));
				config.setHandleFlag(rs.getString("logic_handle_flag"));
				String period = rs.getString("rolling_period");
				config.setSourceName(rs.getString("table_schema"));
				if (StringUtils.isBlank(period)) {
					period = DEFAULT_ROLLING_INTERVAL;
				}
				config.setRollingPeriod(period);
				map.put(tableName, config);

				TableRollingBean rollingBean = new TableRollingBean();
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				Date lastRollingTime = sdf.parse(rs.getString("last_rolling_time"));
				rollingBean.setLastRollingTime(lastRollingTime);
				rollingBean.setNextRollingTime(TimeUtil.getNextTime(
						Double.valueOf(period), 1, lastRollingTime));
				RollingBeanContainer.setRollingBean(tableName, rollingBean);
			}
		} catch (Exception e) {
			logger.error(e.toString(),e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
		}
		return map;
	}
}
