package com.icip.das.core.data;

import java.util.Date;


/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 下午3:47:08 
 * @update	
 */
public class TableConfigBean implements java.io.Serializable{
	
	private static final long serialVersionUID = -5523979492518430880L;

	/** 表名    */
	private String tableName;
	
	/** 唯一条件  */
	private String uniqueCondition;

	/** 数据schema名   */
	private String sourceName;
	
	/** 轮询时间间隔单位"秒",未配置默认30秒  */
	private String rollingPeriod;

	/** rolling_config_custom自定义的查询sql */
	private String customerSql;
	
	/** 是否需要storm处理，不需要时直接存入redis,0否1是  */
	private String handleFlag;
	
	
	/** 最新轮询时间   */
	private Date   lastRollingTime;

	/** 下次轮询时间   */
	private Date nextRollingTime;
	
	/** 临时时间 没有异常时清空,发生异常时根据此时间回滚  */
	private volatile Date temp;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getLastRollingTime() {
		return lastRollingTime;
	}

	public void setLastRollingTime(Date lastRollingTime) {
		this.lastRollingTime = lastRollingTime;
	}

	public Date getNextRollingTime() {
		return nextRollingTime;
	}

	public void setNextRollingTime(Date nextRollingTime) {
		this.nextRollingTime = nextRollingTime;
	}

	public Date getTemp() {
		return temp;
	}

	public void setTemp(Date temp) {
		this.temp = temp;
	}
	
	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getCustomerSql() {
		return customerSql;
	}

	public void setCustomerSql(String customerSql) {
		this.customerSql = customerSql;
	}
	
	public String getUniqueCondition() {
		return uniqueCondition;
	}

	public void setUniqueCondition(String uniqueCondition) {
		this.uniqueCondition = uniqueCondition;
	}

	public String getHandleFlag() {
		return handleFlag;
	}

	public void setHandleFlag(String handleFlag) {
		this.handleFlag = handleFlag;
	}
	
	public String getRollingPeriod() {
		return rollingPeriod;
	}

	public void setRollingPeriod(String rollingPeriod) {
		this.rollingPeriod = rollingPeriod;
	}

	public class FieldMappingConfig{

		private String regexKey;
		
		private String targetKey;
		
		private String regexValue;
		
		private String targetValue;
		
		public String getRegexKey() {
			return regexKey;
		}

		public void setRegexKey(String regexKey) {
			this.regexKey = regexKey;
		}

		public String getTargetKey() {
			return targetKey;
		}

		public void setTargetKey(String targetKey) {
			this.targetKey = targetKey;
		}

		public String getTargetValue() {
			return targetValue;
		}

		public void setTargetValue(String targetValue) {
			this.targetValue = targetValue;
		}
		
		public String getRegexValue() {
			return regexValue;
		}

		public void setRegexValue(String regexValue) {
			this.regexValue = regexValue;
		}

	}
	
	public class BlankMappingConfig{
		
		/** 检查可能为空的字段key值 */
		private String key;
		
		/** 字段为空时替换的值，如果没有此值将被过滤 */
		private String blankValue;
		
		public String getBlankValue() {
			return blankValue;
		}

		public void setBlankValue(String blankValue) {
			this.blankValue = blankValue;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

	}
	
	public class MergeMappingConfig{

		/** 要合并 的key值以英文|分割  */
		private String keyList;
		
		/** 合并后key值  */
		private String mergeKey;
		
		public String getKeyList() {
			return keyList;
		}

		public void setKeyList(String keyList) {
			this.keyList = keyList;
		}

		public String getMergeKey() {
			return mergeKey;
		}

		public void setMergeKey(String mergeKey) {
			this.mergeKey = mergeKey;
		}
	}
	
}
