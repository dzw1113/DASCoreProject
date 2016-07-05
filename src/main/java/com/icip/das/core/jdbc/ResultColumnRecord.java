package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * @Description: 结果集和字段的关联关系,对应result_column_mapping
 * @author  
 * @date 2016年3月18日 上午10:01:39 
 * @update	
 */
public class ResultColumnRecord {
	
	private static final Logger logger = LoggerFactory.getLogger(ResultColumnRecord.class);
	
	public static void updateRelation(String tableName,Map<String,String> param){
		Connection connection = DataSourceUtil.beginTrans("DAS");
		Statement stmt = null;

		try{
			stmt = connection.createStatement();
			String deletesql = "delete from result_column_mapping where result_set=" + "'" + tableName + "'";  
			stmt.executeUpdate(deletesql);
	            
			for (Map.Entry<String, String> entry : param.entrySet()){
				String key = entry.getKey();
				StringBuilder sql = new StringBuilder("INSERT INTO result_column_mapping (result_set,column_name) VALUES(");
				sql.append("'").append(tableName).append("',").append("'").append(key).append("')");
				stmt.execute(sql.toString());
			}
			DataSourceUtil.commitTrans("DAS");
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
