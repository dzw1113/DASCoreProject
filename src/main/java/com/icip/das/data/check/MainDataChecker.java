package com.icip.das.data.check;

import com.icip.das.core.jdbc.DataInitLoader;
import com.icip.das.core.jdbc.DataSourceLoader;
import com.icip.das.core.redis.RedisClientPool;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月21日 上午11:28:14 
 * @update	
 */
public class MainDataChecker {

	public static void main(String[] args) {
		DataSourceLoader.DATASOURCE_INSTANCE.init();//数据源
		DataInitLoader.getTableConfig();//配置
		RedisClientPool.getInstance();

		CheckMysqlData.handleMysqlData(args);
		CheckRedisData.handleRedisKey(args);
		
		SaveErrorData.save();
		System.out.println("结束------------------>");
	}
	
}
