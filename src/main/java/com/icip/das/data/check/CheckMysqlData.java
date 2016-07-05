package com.icip.das.data.check;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceUtil;
import com.icip.das.core.jdbc.QueryDataUtil;
import com.icip.das.util.TimeUtil;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月21日 上午10:31:05 
 * @update	
 */
public class CheckMysqlData {

	private static final Logger logger = LoggerFactory.getLogger(CheckMysqlData.class);
	
	private static final String INIT_TIME = "20151001120000";//首次同步时间
	
	private static final int LIMIT_NO = 5000;
	
	public static void handleMysqlData(String[] tableNames){
		List<String> keysList = new ArrayList<String>();
		for(String tableName : tableNames){
			TableConfigBean configBean = DataInitLoader.getTableConfig().get(tableName);
			if(null == configBean){
				logger.error("error:没有找到TableConfigBean--------------->" + tableName);
			}else{
				String sqlNoLimit = QueryDataUtil.getQuerySql(configBean,TimeUtil.formatString(INIT_TIME));
				long totalNum = QueryDataUtil.getTotalNo(configBean.getSourceName(),tableName,sqlNoLimit);//共需同步数
		        long recordNum = getRecordNum(totalNum);

				int offset = 0;
				for(int i = 0; i < recordNum; i ++){
					String sql = sqlNoLimit;
					offset = LIMIT_NO * i;
					sql = sqlNoLimit + " limit " + String.valueOf(offset) + "," + LIMIT_NO;
					try{
						keysList = queryRemote(configBean,sql);
						insertData(tableName,keysList);
					}catch(Exception ex){
						logger.error("error:出错--------->" + tableName);
						logger.error(ex.toString(),ex);
					}
				}
			}
		}
	}	

	private static long getRecordNum(long totalNum){
		long recordNum = (long) Math.ceil((totalNum / LIMIT_NO));
		if(0 != totalNum % LIMIT_NO)
			return (recordNum + 1);
		else
			return (recordNum);
	}

	private static List<String> queryRemote(TableConfigBean config,String querySql)throws Exception{
		String sql = config.getCustomerSql();//数据库中是否配置自定义sql作为判断依据
		if(!StringUtils.isBlank(sql))
			return queryCustomer(config.getSourceName(),querySql,config.getUniqueCondition());//返回自定义查询结果
					
		return query(config.getSourceName(),querySql);
	}
	
	private static List<String> queryCustomer(String sourceName,String sql,String uniqueCondition)throws Exception {
		String[] uniqueConditions = uniqueCondition.split("\\|");
		Connection connection = DataSourceUtil.getConnection(sourceName);
		Statement stmt = null;
		ResultSet rs = null;
		
		List<String> keysList = new ArrayList<String>();
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			ResultSetMetaData rsm = rs.getMetaData();
			while (rs.next()) {
				Map<String,String> param = new HashMap<String,String>();
				for (int j = 1; j <= rsm.getColumnCount(); j++) {//遍历此条记录的所有字段名--字段值
					param.put(toLowerCase(rsm.getColumnName(j)), rs.getString(rsm.getColumnName(j)));//默认转为驼峰标识
				}
				//唯一条件
				StringBuilder str = new StringBuilder();
				for(int i = 0; i < uniqueConditions.length; i++){
					if(i == (uniqueConditions.length - 1)){
						str.append(param.get(toLowerCase(uniqueConditions[i])));
					}else{
						str.append(param.get(toLowerCase(uniqueConditions[i]))).append(":");
					}
				}
				keysList.add(str.toString());
			}
			return keysList;
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
		 return keysList;
	}
	
	private static List<String> query(String sourceName,String sql)throws Exception {
		Connection connection = DataSourceUtil.getConnection(sourceName);
		Statement stmt = null;
		ResultSet rs = null;
		
		List<String> syncList = new ArrayList<String>();
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				syncList.add(rs.getString("ROLLING_UNIQUE_CONDITION"));
			}
			return syncList;
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
		return syncList;
	}
	
	private static String toLowerCase(String stringes) {
		if (StringUtils.isBlank(stringes))
			return null;
		String[] parts = stringes.toLowerCase().trim().split("_");
		String resultTemp = "";
		for (int i = 0; i < parts.length; i++) {
			resultTemp = resultTemp.concat(firstCharToUpper(parts[i]));
		}
		char[] chares = resultTemp.toCharArray();
		chares[0] = Character.toLowerCase(chares[0]);
		return new String(chares);
	}
	
	private static String firstCharToUpper(String stringes) {
		char[] chares = stringes.toLowerCase().toCharArray();
		chares[0] = Character.toUpperCase(chares[0]);
		return new String(chares);
	}

	private static void insertData(String tableName,List<String> keysList)throws Exception {
		String insertSql = getInsertSql(tableName,keysList);
		insertData(insertSql);
	}
	
	private static String getInsertSql(String tableName,
			List<String> keys) {
		int temp = 0;
		StringBuilder base = new StringBuilder("insert into TMP_MS_CHECK_KEY (RS_KEY,RS_VAL) values ");
		for(String key : keys){
			base.append("( ").append("'").append(tableName).append("'").
			append(",").append("'").append(key).append("'");

			if(temp == keys.size() -1){
				base.append("); ");
			}else{
				base.append("), ");
				temp++;
			}
		}
		return base.toString();
	}
	
	private static void insertData(String insertSql)throws Exception {
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate(insertSql);
			
		}catch(Exception e){
			logger.error(e.toString(),e);
		}finally{
			if(rs != null){
				try{   
					rs.close() ;   
				}catch(SQLException e){   
					logger.error(e.toString(),e);
				}   
			}   
			if(stmt != null){
				try{   
					stmt.close() ;   
				}catch(SQLException e){   
					logger.error(e.toString(),e);
				}   
			}   
			DataSourceUtil.close();
		}
	}
	
}
