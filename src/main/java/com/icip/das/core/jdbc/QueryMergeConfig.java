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
import com.icip.das.core.data.TableConfigBean.MergeMappingConfig;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月9日 下午1:46:02 
 * @update	
 */
public class QueryMergeConfig {

	private static final Logger logger = LoggerFactory.getLogger(QueryMergeConfig.class);
	
	public static List<MergeMappingConfig> getConfig(TableConfigBean tableConfig){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<MergeMappingConfig> configList = new ArrayList<MergeMappingConfig>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select * from rolling_merge_mapping where table_name=" + "'" + tableConfig.getTableName() + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				MergeMappingConfig config = new TableConfigBean().new MergeMappingConfig();
				config.setKeyList(rs.getString("merge_field"));
				config.setMergeKey(rs.getString("merge_key"));
				configList.add(config);
			}
//			tableConfig.setMergeMapping(configList);
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
