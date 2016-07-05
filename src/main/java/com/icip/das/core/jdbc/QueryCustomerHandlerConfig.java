package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年3月18日 下午1:42:51 
 * @update	
 */
public class QueryCustomerHandlerConfig {

	private static final Logger logger = LoggerFactory.getLogger(QueryCustomerHandlerConfig.class);

	public static List<String> getConfig(TableConfigBean tableConfig){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<String> configClass = new ArrayList<String>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select handlerClass from rolling_custom_handler where table_name=" + "'" + tableConfig.getTableName() + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				String handleClass = rs.getString("handlerClass");
				configClass.add(handleClass);
			}
			return configClass;
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
		return configClass;
	}
	
}
