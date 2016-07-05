package com.icip.das.core.thread;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.DataServer;
import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.data.TableRollingBean;
import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.QueryDataUtil;
import com.icip.das.util.TimeUtil;

/** 
 * @Description: TODO
 * @author  yzk
 * @date 2016年3月7日 上午10:33:35 
 * @update	
 */
public class DispatcherRollingTask extends Thread{

	private static final Logger logger = LoggerFactory.getLogger(DispatcherRollingTask.class);
	
	private static final int LIMIT_NO = 2000;
	
	private static final String INIT_TIME = "20151001120000";
	
	public static void dispach(){
		long currentTimeMills = System.currentTimeMillis();
		Date currentTime = new Date(currentTimeMills);
		for(Map.Entry<String, TableConfigBean> entry : DataInitLoader.getTableConfig().entrySet()){
			TableConfigBean config = entry.getValue();
			String tableName = config.getTableName();
			TableRollingBean rollingConfig = RollingBeanContainer.getRollingBean(tableName);
			if(null == rollingConfig){
				continue;
			}
			
			Date lastRollingTime = TimeUtil.formatString(INIT_TIME);
			
			String sqlNoLimit = QueryDataUtil.getQuerySql(config,lastRollingTime);
			long totalNum = QueryDataUtil.getTotalNo(config.getSourceName(),tableName,sqlNoLimit);//共需同步数
			if(0 == totalNum){
				continue;
			}
	        long recordNum = getRecordNum(totalNum);
	        logger.info(config.getTableName() + "   " + currentTime + "   数据共需要的数据量：" + totalNum);
	        logger.info(config.getTableName() + "   " + currentTime + "   数据共需同步任务数：" + recordNum);

	        try{
	        	 int offset = 0;
	        	 for(int i = 0; i < recordNum; i ++){
	        		 String sql = sqlNoLimit;
	        		 offset = LIMIT_NO * i;
	        		 sql = sqlNoLimit + " limit " + String.valueOf(offset) + "," + LIMIT_NO;
	        		 logger.debug(config.getTableName() + "   " + currentTime + "  第：" + (i + 1) + "次" + "同步sql ---> " +  sql);
	        		 if(i == recordNum - 1){
	        			 DataServer.SERVER_INSTANCE.handleData(config, -1,sql,true);//TODO 做死了6小时超时
	        		 }else{
	        			 DataServer.SERVER_INSTANCE.handleData(config, -1,sql,false);
	        		 }
	        	 }
	        }catch(Exception ex){
				//TODO 线程泄露
				logger.error(config.getTableName() + "异常，重置最新轮询时间------");
				logger.error(ex.toString(),ex);
				continue;
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
	
}
