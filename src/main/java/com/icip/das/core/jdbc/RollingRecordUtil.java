package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableRollingBean;
import com.icip.das.util.TimeUtil;

/** 
 * @Description: 数据库保存的最新更新时间
 * @author  yzk
 * @date 2016年3月8日 下午5:28:48 
 * @update	
 */
public class RollingRecordUtil {

	private static final Logger logger = LoggerFactory.getLogger(RollingRecordUtil.class);
	
	public static synchronized void updateRollingTime(TableConfigBean config){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		
		String tableName = config.getTableName();
		TableRollingBean rollingBean = RollingBeanContainer.getRollingBean(tableName);
		if(null == rollingBean){
			return;
		}
		String updateTime = TimeUtil.formatDate(rollingBean.getLastRollingTime());
		String sql = config.getCustomerSql();
		try{
			stmt = connection.createStatement();
			if(StringUtils.isBlank(sql)){
				stmt.executeUpdate("update rolling_config set last_rolling_time=" 
						+ updateTime +" where table_Name=" + "'" + tableName + "'");
			}else{
				stmt.executeUpdate("update rolling_config_custom set last_rolling_time=" 
						+ updateTime +" where visual_table_Name=" + "'" + tableName + "'");
			}
			
		}catch(Exception e){//TODO 数据库更新失败？？？
			logger.error(e.toString(), e);
		}finally{
			if(stmt != null){
				try{   
					stmt.close() ;   
				}catch(SQLException e){   
					logger.error(e.toString(), e);
				}   
			}   
			DataSourceUtil.close();
		}
	}
	
}
