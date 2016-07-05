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
import com.icip.das.core.data.TableConfigBean.FieldMappingConfig;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月9日 下午2:49:02 
 * @update	
 */
public class QueryFieldMappingConfig {

	private static final Logger logger = LoggerFactory.getLogger(QueryFieldMappingConfig.class);

	public static List<FieldMappingConfig> getConfig(TableConfigBean tableConfig){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<FieldMappingConfig> configList = new ArrayList<FieldMappingConfig>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select * from rolling_field_mapping where table_name=" + "'" + tableConfig.getTableName() + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				FieldMappingConfig config = new TableConfigBean().new FieldMappingConfig();
				config.setRegexKey(rs.getString("regex_key"));
				config.setTargetKey(rs.getString("target_key"));
				config.setRegexValue(rs.getString("regex_value"));
				config.setTargetValue(rs.getString("target_value"));
				
				configList.add(config);
			}
//			tableConfig.setFiledMapping(configList);
			return configList;
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
		 return configList;
	}
	
}
