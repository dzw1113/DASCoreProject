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
 * @Description: 误删
 * @author  yzk
 * @date 2016年3月7日 上午10:33:35 
 * @update	
 */
//FIXME 此类误删
public class RollingMonitorTread extends Thread{

	private static final Logger logger = LoggerFactory.getLogger(RollingMonitorTread.class);
	
	public static volatile boolean stop = false;
	
	private static final int LIMIT_NO = 2000;

	public static volatile boolean IS_RUNNING = false;
	
	public void run(){
		IS_RUNNING = true;
		logger.info("--------启动轮询检查器 RollingMonitorTread-------->");
		for(;;){
			long currentTimeMills = System.currentTimeMillis();
			Date currentTime = new Date(currentTimeMills);
			if (stop)
				break;
			
			for(Map.Entry<String, TableConfigBean> entry : DataInitLoader.getTableConfig().entrySet()){
				TableConfigBean config = entry.getValue();
				String tableName = config.getTableName();
				TableRollingBean rollingConfig = RollingBeanContainer.getRollingBean(tableName);
				if(null == rollingConfig){
					continue;
				}
				
				Date nextRollingTime = rollingConfig.getNextRollingTime();
				if(currentTime.equals(nextRollingTime) //当前时间等于下次轮询时间,或当前时间在轮询时间之后
						|| currentTime.after(nextRollingTime)){
					
					Date lastRollingTime = rollingConfig.getLastRollingTime();//这个最新轮询时间只作为拼接sql的参数,会被覆盖掉
					String period = config.getRollingPeriod();
					
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
			        			 //FIXME 最后一次更新RollingBeanContainer里记录的时间，如果执行时子线程内出现异常kafka会出现重复数据
			        			 rollingConfig.setNextRollingTime(TimeUtil.getNextTime(Double.valueOf(period),1,currentTime));//覆盖掉下次轮询时间
			        			 rollingConfig.setLastRollingTime(currentTime);//设置最新轮询时间为当前执行时间
			        			 RollingBeanContainer.updateRollingBean(tableName,rollingConfig);
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
		}
	}
	
	private long getRecordNum(long totalNum){
		long recordNum = (long) Math.ceil((totalNum / LIMIT_NO));
		if(0 != totalNum % LIMIT_NO)
			return (recordNum + 1);
		else
			return (recordNum);
	}
	
}
