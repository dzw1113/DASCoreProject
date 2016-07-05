package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.util.TimeUtil;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年3月22日 下午4:15:36 
 * @update	
 */
public class DataBatchUtil {

	private static final Logger logger = LoggerFactory.getLogger(DataBatchUtil.class);
	
	private static final String BATCH_TIME = "20151001120000";

	/**
	 *	取批量配置表所有记录 
	 */
	public static List<Map<String,String>> getAllConfig(){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<Map<String,String>> allTableConfig = new ArrayList<Map<String,String>>();
		try{
			String queryStr = "select * from batch_config";
			stmt = connection.createStatement();
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				Map<String,String> tableConfig = new HashMap<String,String>();
				tableConfig.put("tableName", rs.getString("table_name"));
				tableConfig.put("period", rs.getString("period"));
				tableConfig.put("lastBatchingTime", rs.getString("last_batching_time"));
				
				allTableConfig.add(tableConfig);
			}
			return allTableConfig;
		}catch(Exception e){
			logger.error(e.toString(), e);
		}finally{
			if(rs != null){
				try{   
					rs.close() ;   
				}catch(SQLException e){   
					logger.error(e.toString(), e);
				}   
			}   
			if(stmt != null){
				try{   
					stmt.close() ;   
				}catch(SQLException e){   
					logger.error(e.toString(), e);
				}   
			}   
			DataSourceUtil.close();
		}
		 return allTableConfig;
	}
	
	/**
	 *	重置轮询时间(全量)
	 */
	public static void resetRollingTime(String tableName,String currentTime){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate("update rolling_config set last_rolling_time=" 
					+ BATCH_TIME +" where table_Name=" + "'" + tableName + "'");
			stmt.executeUpdate("update rolling_config_custom set last_rolling_time=" 
					+ BATCH_TIME +" where visual_table_Name=" + "'" + tableName + "'");
			
			stmt.executeUpdate("update batch_config set last_batching_time=" 
					+ currentTime +" where table_name=" + "'" + tableName + "'");
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
	
	public static void main(String[] args) {
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		Date currentTime = new Date(System.currentTimeMillis());
		resetRollingTime("vt_bill1",TimeUtil.formatDate(currentTime));
	}
}
