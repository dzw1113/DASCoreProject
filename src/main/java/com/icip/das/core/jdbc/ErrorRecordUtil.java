package com.icip.das.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.ErrorRecordBean;
import com.icip.das.core.data.TableConfigBean;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月21日 下午7:34:52 
 * @update	
 */
public class ErrorRecordUtil {

	private static final Logger logger = LoggerFactory.getLogger(ErrorRecordUtil.class);

	/**
	 *	查询数据时异常errorType=0
	 */
	public static void saveQueryErrorSql(TableConfigBean config,String excuteSql){
		Connection connection = DataSourceUtil.beginTrans("DAS");
		Statement stmt = null;
		
		String tableName = config.getTableName();
		String handleFlag = config.getHandleFlag();
		if(!StringUtils.isBlank(handleFlag) && "1".equals(handleFlag)){
			handleFlag = "1";
		}else{
			handleFlag = "0";
		}
		StringBuilder sql = new StringBuilder("INSERT INTO `rolling_error_record` (`table_Name`,`sql`,`flag`,`logic_handle_flag`,`errorType`) VALUES(");
		sql.append("'").append(tableName).append("',");
		sql.append("'").append(excuteSql).append("',").append("'0'").append(",'").append(handleFlag).append("','0')");
		
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate(sql.toString());
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
	
	/**
	 *	存入redis时异常errorType=1
	 */
	public static void saveRedisError(TableConfigBean config,String redisKey,Map<String,String> redisValue){
		String data = JSONObject.toJSONString(redisValue);
		
		String tableName = config.getTableName();
		String handleFlag = config.getHandleFlag();
		if(!StringUtils.isBlank(handleFlag) && "1".equals(handleFlag)){
			handleFlag = "1";
		}else{
			handleFlag = "0";
		}
		
		StringBuilder sql = new StringBuilder("INSERT INTO `rolling_error_record` (`table_Name`,`redisKey`,`redisValue`,`flag`,`logic_handle_flag`,`errorType`) VALUES(");
		sql.append("'").append(tableName).append("',");
		sql.append("'").append(redisKey).append("','").append(data).append("',").append("'0','").append(handleFlag).append("','1')");
		
		Connection connection = DataSourceUtil.beginTrans("DAS");
		Statement stmt = null;
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate(sql.toString());
			DataSourceUtil.commitTrans("DAS");
		}catch(Exception e){
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
	
	/**
	 *	kafka通知时异常errorType=2
	 */
	public static void saveKafkaError(String tableName,String redisKey){
		StringBuilder sql = new StringBuilder("INSERT INTO `rolling_error_record` (`table_Name`,`redisKey`,`flag`,`logic_handle_flag`,`errorType`) VALUES(");
		sql.append("'").append(tableName).append("',");
		sql.append("'").append(redisKey).append("',").append("'0',").append("'1'").append(",'2')");
		
		Connection connection = DataSourceUtil.beginTrans("DAS");
		Statement stmt = null;
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate(sql.toString());
			DataSourceUtil.commitTrans("DAS");
		}catch(Exception e){
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

	/**
	 * @Description: 取数据库中记录的失败任务总数
	 * @param    
	*/
	public static long getTotalErrorTaskNo() {
		String queryStr = "select count(1) from das.rolling_error_record where flag='0' ";
		
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		int temp = 0;
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(queryStr);
			
			while (rs.next()) {
				temp = temp + (rs.getInt("count(1)"));
			}
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
		return temp;
	}
	
	/**
	 * @Description: 取数据库中记录的失败任务
	 * @param    
	*/
	public static List<ErrorRecordBean> getErrorTask(String offset,String limitNo) {
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;

		List<ErrorRecordBean> recordList = new ArrayList<ErrorRecordBean>();
		try {
			stmt = connection.createStatement();
			String queryStr = "select * from das.rolling_error_record where flag='0' limit " + offset + "," + limitNo;
			rs = stmt.executeQuery(queryStr);

			while (rs.next()) {
				ErrorRecordBean errorRecord = new ErrorRecordBean();
				errorRecord.setPk(rs.getString("pk"));
				errorRecord.setTableName(rs.getString("table_Name"));
				errorRecord.setErrorType(rs.getString("errorType"));
				errorRecord.setExcuteSql(rs.getString("sql"));
				errorRecord.setFlag(rs.getString("flag"));
				errorRecord.setLogicHandleFlag(rs.getString("logic_handle_flag"));
				errorRecord.setRedisKey(rs.getString("redisKey"));
				errorRecord.setRedisValue(rs.getString("redisValue"));
				
				recordList.add(errorRecord);
			}
		} catch (Exception e) {
			logger.error(e.toString(), e);
		} finally {
			if (rs != null) {// 关闭记录集
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			if (stmt != null) { // 关闭声明
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e.toString(), e);
				}
			}
			DataSourceUtil.close();
		}
		return recordList;
	}
	
	/**
	 *	重试失败任务后更新处理标志
	 */
	public static void updateFlag(String pk){
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate("update rolling_error_record set flag=1 where pk =" 
					 + "'" + pk + "'");
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
