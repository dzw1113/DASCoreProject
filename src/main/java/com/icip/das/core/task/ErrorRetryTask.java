package com.icip.das.core.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.icip.das.core.DataServer;
import com.icip.das.core.data.ErrorRecordBean;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.ErrorRecordUtil;
import com.icip.das.core.kafka.KafkaProducer;
import com.icip.das.core.redis.RedisClient;

/** 
 * @Description: 异常重试
 * @author  
 * @date 2016年4月22日 上午10:07:26 
 * @update	
 */
public class ErrorRetryTask extends TimerTask{

	private static final Logger logger = LoggerFactory.getLogger(ErrorRetryTask.class);

	private static final int LIMIT_NO = 2000;

	@Override
	public void run() {
		logger.info("ErrorRetryTask------检查失败重试----->");
		long totalNum = ErrorRecordUtil.getTotalErrorTaskNo();
		if(0 == totalNum){
			return;
		}
		
		logger.info("ErrorRetryTask------失败记录共："+ totalNum +"----->");
        long recordNum = getRecordNum(totalNum);
        for(int i = 0; i < recordNum; i ++){
        	List<ErrorRecordBean> records = ErrorRecordUtil.getErrorTask(String.valueOf(i * LIMIT_NO),String.valueOf((i + 1) * LIMIT_NO));
        	retryTask(records);
        }
	}

	private void retryTask(List<ErrorRecordBean> records) {
		for(ErrorRecordBean record : records){
			switch(record.getErrorType()){
			case "0":
				retryQuerySql(record);
				break;
			case "1":
				retryRedis(record);
				break;
			case "2" :
				retryKafka(record);
				break;
			}
		}
	}

	private void retryKafka(ErrorRecordBean record) {
		String pk = record.getPk();
		String tableName = record.getTableName();
		String redisKey = record.getRedisKey();
		try{
			List<String> list = new ArrayList<String>();
			list.add(redisKey);
			KafkaProducer producer = new KafkaProducer(tableName,list);
			producer.send();
		}catch(Exception e){
			logger.error("重试任务：" + pk + " 失败。。。");
			return;
		}
		ErrorRecordUtil.updateFlag(pk);
	}

	@SuppressWarnings("unchecked")
	private void retryRedis(ErrorRecordBean record) {
		String pk = record.getPk();
		String tableName = record.getTableName();
		String redisKey = record.getRedisKey();
		String redisValueJson = record.getRedisValue();
		
		Map<String,String> param = JSONObject.parseObject(redisValueJson, java.util.Map.class);
		try{
			RedisClient.setDataToRedis(redisKey,param);
		}catch(Exception ex){
			logger.error("重试任务：" + pk + " 失败。。。");
			return; 
		}
		
//		try{
//			String handleFlag = record.getLogicHandleFlag();
//			if(!StringUtils.isBlank(handleFlag) && "1".equals(handleFlag)){
//				List<String> list = new ArrayList<String>();
//				list.add(redisKey);
//				KafkaProducer producer = new KafkaProducer(tableName,list);
//				producer.send();
//			}
//		}catch(Exception e){
//			ErrorRecordUtil.saveKafkaError(tableName,redisKey);
//			logger.error("重试redis任务：" + pk + " kafka通知失败。。。");
//			logger.error(e.toString(),e);
//		}
		
	}

	private void retryQuerySql(ErrorRecordBean record) {
		String pk = record.getPk();
		String tableName = record.getTableName();
		String sql = record.getExcuteSql();
		TableConfigBean config = DataInitLoader.getTableConfig().get(tableName);
		try{
			DataServer.SERVER_INSTANCE.handleData(config, -1,sql,false);
		}catch(Exception ex){
			logger.error("重试任务：" + pk + " 失败。。。");
			return; 
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
