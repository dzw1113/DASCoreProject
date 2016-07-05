package com.icip.das.core.data;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月22日 上午10:02:11 
 * @update	
 */
public class ErrorRecordBean {
	
	private String pk;
	
	private String tableName;

	private String excuteSql;
	
	private String redisKey;
	
	private String redisValue;
	
	private String flag;
	
	private String logicHandleFlag;

	private String errorType;

	public String getPk() {
		return pk;
	}

	public void setPk(String pk) {
		this.pk = pk;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getExcuteSql() {
		return excuteSql;
	}

	public void setExcuteSql(String excuteSql) {
		this.excuteSql = excuteSql;
	}

	public String getRedisKey() {
		return redisKey;
	}

	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	public String getRedisValue() {
		return redisValue;
	}

	public void setRedisValue(String redisValue) {
		this.redisValue = redisValue;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public String getErrorType() {
		return errorType;
	}

	public void setErrorType(String errorType) {
		this.errorType = errorType;
	}
	
	public String getLogicHandleFlag() {
		return logicHandleFlag;
	}

	public void setLogicHandleFlag(String logicHandleFlag) {
		this.logicHandleFlag = logicHandleFlag;
	}
	
}
