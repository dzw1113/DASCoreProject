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
import com.icip.das.core.data.TableConfigBean.BlankMappingConfig;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月9日 下午1:01:53 
 * @update	
 */
public class QueryBlankMappingConfig {

	private static final Logger logger = LoggerFactory.getLogger(QueryBlankMappingConfig.class);
	
	public static List<BlankMappingConfig> getConfig(TableConfigBean tableConfig){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<BlankMappingConfig> configList = new ArrayList<BlankMappingConfig>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select * from rolling_blank_mapping where table_name=" + "'" + tableConfig.getTableName() + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				BlankMappingConfig config = new TableConfigBean().new BlankMappingConfig();
				config.setKey(rs.getString("table_column"));
				config.setBlankValue(rs.getString("blank_value"));
				configList.add(config);
			}
//			tableConfig.setBlankMapping(configList);
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
