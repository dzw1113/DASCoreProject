package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.FilterBean;
import com.icip.das.core.data.TableConfigBean;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月10日 下午3:30:48 
 * @update	 
 */
public class QueryFilterConfig {

	private static final Logger logger = LoggerFactory.getLogger(QueryFilterConfig.class);

	public static Map<String,FilterBean> getConfig(TableConfigBean tableConfig){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		Map<String,FilterBean> configMap = new HashMap<String,FilterBean>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select * from rolling_column_filter where table_name=" + "'" + tableConfig.getTableName() + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				FilterBean filterConfig = new FilterBean();
				filterConfig.setLogic(rs.getString("logic"));
				filterConfig.setRange(rs.getString("range"));
				filterConfig.setRegex(rs.getString("regex"));
				filterConfig.setRegular(rs.getString("regular"));
				filterConfig.setMatch(rs.getString("match"));
				
				configMap.put(rs.getString("table_column"), filterConfig);
			}
			return configMap;
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
		return configMap;
	}
	
	public static void main(String[] args)throws Exception {
		DataSourceLoader.DATASOURCE_INSTANCE.init();
		
		TableConfigBean config = new TableConfigBean();
		config.setTableName("icip_sys_para");
		
		getConfig(config);
	}
	
}
