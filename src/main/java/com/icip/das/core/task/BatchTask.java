package com.icip.das.core.task;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.icip.das.core.data.RollingBeanContainer;
import com.icip.das.core.jdbc.DataBatchUtil;
import com.icip.das.core.redis.RedisClient;
import com.icip.das.core.redis.RedisClientPool;
import com.icip.das.util.TimeUtil;

/** 
 * @Description: 批量定时任务（实际执行时间误差半小时）
 * @author  
 * @date 2016年3月22日 下午12:04:23 
 * @update	
 */
public class BatchTask extends TimerTask{

	private static final Logger logger = LoggerFactory.getLogger(BatchTask.class);

	public void run() {
		logger.info("BatchTask------轮询定时配置----->");
		try {
			List<Map<String,String>> allConfig = DataBatchUtil.getAllConfig();
			for(Map<String,String> config : allConfig){
				String lastBacthConfig = config.get("lastBatchingTime");
			
				Date lastBatch = TimeUtil.formatString(lastBacthConfig);
				Double period = Double.valueOf(config.get("period"));
				Date nextBatchTime = TimeUtil.getNextTime(period,4,lastBatch);

				Date currentTime = new Date(System.currentTimeMillis());
				if(currentTime.equals(nextBatchTime)
						|| currentTime.after(nextBatchTime)){
					String tableName = config.get("tableName");
					logger.info("批量处理--------------->" + tableName);
					//TODO 下面3个动作危险！
					if(!StringUtils.isBlank(tableName)){
						DataBatchUtil.resetRollingTime(tableName,TimeUtil.formatDate(currentTime));
						clearRedisRecord(tableName);
						RollingBeanContainer.removeRollingBean(tableName);
					}
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	/**
	 *	清空redis记录 
	 */
	private static void clearRedisRecord(String tableName) {
		Jedis redisPipeClient = null;
		try {
			redisPipeClient = RedisClientPool.getResource();
			Pipeline pipe = redisPipeClient.pipelined();
			Set<String> keys = redisPipeClient.keys(tableName + ":*");
			if(null == keys || keys.size() == 0){
				return;
			}
			
			String[] keysArray = keys.toArray(new String[]{});
			pipe.sync(); 

			RedisClient.deleteDatas(keysArray);
			logger.info("-------批量任务，删除redis中" + tableName + "表数据，共" + keys.size() + "条");
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}finally{
			if(null != redisPipeClient){
				redisPipeClient.close();
			}
		}
	}
	
	public static void main(String[] args) {
		clearRedisRecord("bd_community_base_info");
	}
}
