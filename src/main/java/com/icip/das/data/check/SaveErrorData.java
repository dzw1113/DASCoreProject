package com.icip.das.data.check;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.jdbc.DataSourceUtil;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月21日 上午11:39:52 
 * @update	
 */
public class SaveErrorData {

	private static final Logger logger = LoggerFactory.getLogger(SaveErrorData.class);
	
	private static final int LIMIT_NO = 5000;
	
//	0 redis没有，1mysql没有
	public static void save(){
		List<Map<String,String>> notInRedisDatas = getDataNotInRedis();
		saveErrorDatas(notInRedisDatas);
		List<Map<String,String>> notInMysqlDatas = getDataNotInMysql();
		saveErrorDatas(notInMysqlDatas);
	}
	
	private static void saveErrorDatas(List<Map<String, String>> datas) {
		List<Map<String, String>> tempDatas = new ArrayList<Map<String, String>>();
		long totalNum = datas.size();
        long recordNum = getRecordNum(totalNum);
		for(int offset = 0; offset < recordNum; offset ++){
			if(offset == recordNum -1){
				for(int t = 2000 * offset;t < datas.size(); t++){
					tempDatas.add(datas.get(t));
				}
			}else{
				for(int t = 2000 * offset;t < 2000 * (offset + 1); t++){
					tempDatas.add(datas.get(t));
				}
			}
			saveDb(tempDatas);
			tempDatas.clear();
		}
        
	}

	private static void saveDb(List<Map<String, String>> datas){
		String insertSql = getInsertSql(datas);
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		
		try{
			stmt = connection.createStatement();
			stmt.executeUpdate(insertSql);
			
		}catch(Exception e){
			System.out.println(e);
		}finally{
			if(rs != null){
				try{   
					rs.close() ;   
				}catch(SQLException e){   
					System.out.println(e);
				}   
			}   
			if(stmt != null){
				try{   
					stmt.close() ;   
				}catch(SQLException e){   
					System.out.println(e);
				}   
			}   
			DataSourceUtil.close();
		}
	}
	
	private static String getInsertSql(
			List<Map<String, String>> datas) {
		int temp = 0;
		StringBuilder base = new StringBuilder("insert into TMP_RESULT_CHECK_KEY (RS_KEY,RS_VAL,RS_FLAG) values ");
		for(Map<String,String> data : datas){
			base.append("( ").append("'").append(data.get("RS_KEY")).append("'").
			append(",").append("'").append(data.get("RS_VAL")).append("'").
			append(",").append("'").append(data.get("RS_FLAG")).append("'");
			
			if(temp == datas.size() -1){
				base.append("); ");
			}else{
				base.append("), ");
				temp++;
			}
		}
		return base.toString();
	}
	
	private static List<Map<String,String>> getDataNotInRedis(){
		String sql = "select * from TMP_MS_CHECK_KEY  a where not exists  (select * from TMP_RD_CHECK_KEY b where a.rs_key = b.rs_key and a.rs_val = b.rs_val)";
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		List<Map<String,String>> errorList = new ArrayList<Map<String,String>>();
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				Map<String,String> param = new HashMap<String,String>();
				param.put("RS_KEY", rs.getString("RS_KEY"));
				param.put("RS_VAL", rs.getString("RS_VAL"));
				param.put("RS_FLAG", "0");
				errorList.add(param);
			}
			return errorList;
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
		return errorList;
	}
	
	private static List<Map<String,String>> getDataNotInMysql(){
		String sql = "select * from TMP_RD_CHECK_KEY a where not exists  (select * from TMP_MS_CHECK_KEY b where a.rs_key = b.rs_key and a.rs_val = b.rs_val)";
		Connection connection = DataSourceUtil.getConnection("DAS");
		Statement stmt = null;
		ResultSet rs = null;
		List<Map<String,String>> errorList = new ArrayList<Map<String,String>>();
		try{
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				Map<String,String> param = new HashMap<String,String>();
				param.put("RS_KEY", rs.getString("RS_KEY"));
				param.put("RS_VAL", rs.getString("RS_VAL"));
				param.put("RS_FLAG", "1");
				errorList.add(param);
			}
			return errorList;
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
		return errorList;
	}
	
	private static long getRecordNum(long totalNum){
		long recordNum = (long) Math.ceil((totalNum / LIMIT_NO));
		if(0 != totalNum % LIMIT_NO)
			return (recordNum + 1);
		else
			return (recordNum);
	}
	
}
