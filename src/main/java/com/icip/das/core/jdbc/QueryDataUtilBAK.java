package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.util.TimeUtil;

/** 
 * @Description: 轮询帮助
 * @author  yzk
 * @date 2016年3月8日 下午3:14:44 
 * @update	
 */
public class QueryDataUtilBAK {
	
	private static final Logger logger = LoggerFactory.getLogger(QueryDataUtilBAK.class);
	
	/**
	 *	获取要执行的查询sql
	 *	注意:调用此方法即刷新内存中对应表的最新轮询时间为传入的轮询时间
	 */
	public static String getQuerySql(TableConfigBean config,String lastRollingTime,String currentRollingTime){
		String sql = config.getCustomerSql();
//		config.setTemp(config.getLastRollingTime());
		if(!StringUtils.isBlank(sql)){
			sql = sql.replace("\r\n"," ").trim();//去除换行符
			List<String> list = recursionStr(sql,lastRollingTime,currentRollingTime,new ArrayList<String>());
			String resultSql = "";
			for(String s : list){
				resultSql = resultSql + s;
			}
			return resultSql;
		}
		
		String tableName = config.getTableName();
		String uniqueCondition = config.getUniqueCondition();
		String queryStr = getQueryStr(tableName,uniqueCondition,lastRollingTime,currentRollingTime);//TODO 
		return queryStr;
	}
	
	/**
	 * 拼接非自定义查询sql
	 * 查询时间updateTime > lastRollingTime and updateTime <= limitRollingTime
	 */
	private static String getQueryStr(String tableName,String uniqueCondition,String lastRollingTime,String limitRollingTime){
		List<String> columnList = getQueryColumn(tableName);
		String[] uniqueConditions = uniqueCondition.split("\\|");
		
		if(null == columnList || columnList.isEmpty()){//未配置要查询的字段，查询所有
			StringBuilder str = new StringBuilder("select a.*,CONCAT_WS(':',");
			for(int i = 0; i < uniqueConditions.length; i++){
				if(i == (uniqueConditions.length - 1)){
					str.append(uniqueConditions[i]);
				}else{
					str.append(uniqueConditions[i]).append(",");
				}
			}
			str.append(") AS ROLLING_UNIQUE_CONDITION from ").append(tableName).append(" a")
			.append(" where a.update_time> ").append(lastRollingTime).append(" and a.update_time <= ").append(limitRollingTime);
			return str.toString();
		}
		
		StringBuilder str = new StringBuilder();//只查询配置的字段
		str.append("select ");
		for(int i = 0; i < columnList.size();i++){
			str.append(columnList.get(i)).append(",");
		}
		str.append("CONCAT_WS(':',");
		for(int i = 0; i < uniqueConditions.length; i++){
			if(i == (uniqueConditions.length - 1)){
				str.append(uniqueConditions[i]);
			}else{
				str.append(uniqueConditions[i]).append(",");
			}
		}
		str.append(") AS ROLLING_UNIQUE_CONDITION from ").append(tableName).append(" where UPDATE_TIME>")
			.append(lastRollingTime).append(" and update_time <= ").append(limitRollingTime);
		return str.toString();
	}
	
	/**
	 * 查询对应数据库目标表在时间lastRollingTime之后的所有数据
	 */
	public static List<Map<String,String>> queryRemote(TableConfigBean config,String querySql)throws Exception{
		String sql = config.getCustomerSql();//数据库中是否配置自定义sql作为判断依据
		if(!StringUtils.isBlank(sql))
			return queryCustomer(config.getSourceName(),querySql,config.getUniqueCondition());//返回自定义查询结果
					
		return query(config.getSourceName(),querySql);
	}
	
	public static List<Map<String, String>> query(String sourceName,String sql) {
		Connection connection = DataSourceUtil.getConnection(sourceName);
		Statement stmt = null;
		ResultSet rs = null;
		
		List<Map<String,String>> syncList = new ArrayList<Map<String,String>>();
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			ResultSetMetaData rsm = rs.getMetaData();
			while (rs.next()) {
				Map<String,String> param = new HashMap<String,String>();
				for (int j = 1; j <= rsm.getColumnCount(); j++) {//遍历此条记录的所有字段名--字段值
					String str = rs.getString(rsm.getColumnName(j));
					param.put(toLowerCase(rsm.getColumnName(j)), StringUtils.isEmpty(str)?"":str);//默认转为驼峰标识
				}
				syncList.add(param);
			}
			return syncList;
		}catch(Exception e){
			logger.error(e.toString(), e);
//			this.interrupt();
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

	private static List<Map<String, String>> queryCustomer(String sourceName,String sql,String uniqueCondition) {
		String[] uniqueConditions = uniqueCondition.split("\\|");
		Connection connection = DataSourceUtil.getConnection(sourceName);
		Statement stmt = null;
		ResultSet rs = null;
		
		List<Map<String,String>> syncList = new ArrayList<Map<String,String>>();
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
				param.put("rollingUniqueCondition", str.toString());
				syncList.add(param);
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

	/**
	 * 查轮询的表中字段配置,如果返回null,查所有
	 */
	private static List<String> getQueryColumn(String tableName){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		List<String> columnList = new ArrayList<String>();
		try{
			stmt = connection.createStatement();
			String queryStr = "select * from rolling_column_rela where table_name=" + "'" + tableName + "'";
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				columnList.add(rs.getString("column_name"));
			}
			return columnList;
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
		 return columnList;
	}
	
	/**
	 *	根据查询sql获取业务系统总记录数 
	 */
	public static long getTotalNo(String sourceName,String tableName,String sql){
		String count = " select count(1) as '$TEMP$' from(";
		String queryStr = count + sql + ") temp";
		
//		String queryStr = " select count(1) as '$TEMP$' from " + tableName;
		
		Connection connection = DataSourceUtil.getConnection(sourceName);
		Statement stmt = null;
		ResultSet rs = null;
//		logger.info("-----getCount sql------>" + queryStr);
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(queryStr);
			
			int temp = 0;
			while (rs.next()) {
				temp = temp + (rs.getInt("$TEMP$"));
			}
			return temp;
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
		return 0;
	}
	
	public static String toLowerCase(String stringes) {
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
	
	private static List<String> recursionStr(String sql,String lastRollingTime,String limitRollingTime, ArrayList<String> sqlPart){
		int bracketIndex = sql.indexOf("{}");//{}索引位置
		if(bracketIndex == -1){
			sqlPart.add(sql);
			return sqlPart;
		}
		
		if(sql.length() > 0){
			String excludeBracket = sql.substring(0, bracketIndex);
			String temp = concatUpdateStr(excludeBracket,lastRollingTime,limitRollingTime);
			sqlPart.add(temp);
		}
		
		String unHandleStr = sql.substring(bracketIndex + 2).trim();//sql中还没处理的部分
		if(unHandleStr.length() > 0){
			recursionStr(unHandleStr,lastRollingTime,limitRollingTime,sqlPart);
		}
		return sqlPart;
	}
	
	private static String concatUpdateStr(String partSql,String lastRollingTime,String limitRollingTime) {
		String aliasName = "";
		String[] splitArray = partSql.split("\\s+");
		String temp = splitArray[splitArray.length - 1];//a.update_time> 
		String[] dotArray = temp.split("\\.");//按"."分隔
		if(dotArray.length > 1){
			aliasName = temp.split("\\.")[0];//取得别名
		}
		
		StringBuilder reStr = new StringBuilder(lastRollingTime);
		reStr.append(" and ");
		if(!StringUtils.isBlank(aliasName)){
			reStr.append(aliasName).append(".");
		}
		reStr.append("update_time <= ").append(limitRollingTime).append(" ");
		
		String result = partSql + reStr.toString();
		return result;
	}
	
	//FIXME ------------------------------------------------------->
	public static String getQuerySqlTemp(TableConfigBean config,Date lastRollingTime){
		String sql = config.getCustomerSql();
		if(!StringUtils.isBlank(sql)){
			sql = sql.replace("\r\n"," ").trim();//去除换行符
			sql = sql.replaceAll("\\{\\}", TimeUtil.formatDate(lastRollingTime));
			return sql;
		}
		
		String tableName = config.getTableName();
		String uniqueCondition = config.getUniqueCondition();
		
		String queryStr = getQueryStrTemp(tableName,uniqueCondition,TimeUtil.formatDate(lastRollingTime));
		return queryStr;
	}
	
	private static String getQueryStrTemp(String tableName,String uniqueCondition,String lastRollingTime){
		List<String> columnList = getQueryColumn(tableName);
		String[] uniqueConditions = uniqueCondition.split("\\|");
		
		if(null == columnList || columnList.isEmpty()){//未配置要查询的字段，查询所有
			StringBuilder str = new StringBuilder("select a.*,CONCAT_WS(':',");
			for(int i = 0; i < uniqueConditions.length; i++){
				if(i == (uniqueConditions.length - 1)){
					str.append(uniqueConditions[i]);
				}else{
					str.append(uniqueConditions[i]).append(",");
				}
			}
			str.append(") AS ROLLING_UNIQUE_CONDITION from ").append(tableName).append(" a")
			.append(" where UPDATE_TIME> ").append(lastRollingTime);
			return str.toString();
		}
		
		StringBuilder str = new StringBuilder();//只查询配置的字段
		str.append("select ");
		for(int i = 0; i < columnList.size();i++){
			str.append(columnList.get(i)).append(",");
		}
		str.append("CONCAT_WS(':',");
		for(int i = 0; i < uniqueConditions.length; i++){
			if(i == (uniqueConditions.length - 1)){
				str.append(uniqueConditions[i]);
			}else{
				str.append(uniqueConditions[i]).append(",");
			}
		}
		str.append(") AS ROLLING_UNIQUE_CONDITION from ").append(tableName).append(" where UPDATE_TIME>").append(lastRollingTime);
		return str.toString();
	}
}
