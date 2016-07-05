package com.icip.das.core.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.framework.constans.DASConstants;
import com.icip.framework.curator.CuratorTools;

/**
 * 
 * @Description: redis辅助类，增入优先从zk抽取配置，抽不到再读本地
 * @author
 * @date 2016年3月22日 上午10:50:51
 * @update
 */
public class RedisPropertiesUtil {

	private static final Logger logger = LoggerFactory.getLogger(RedisPropertiesUtil.class);

	public static final RedisPropertiesUtil PRO_INSTANCE = new RedisPropertiesUtil();

	private Properties pro;

	public Properties getProperties() {
		if (null == pro) {
			synchronized (this) {
//				if (null == pro) {
//					try{
//						this.pro = CuratorTools.downCfgProperties(DASConstants.REDIS_PATH);
//					}catch(Exception e){
//						
//					}
//				}
				if (null == pro) {
					this.pro = loadProperties();
				}
			}
		}
		return pro;
	}

	private Properties loadProperties() {
		Properties props = new Properties();
		FileInputStream in = null;
		try {
			File propertiesFile = new File(this.getClass().getResource("/")
					.getPath()
					+ "redis.properties");
			if (null == propertiesFile || !propertiesFile.exists())
				throw new FileNotFoundException();

			in = new FileInputStream(propertiesFile);
			props.load(in);
		} catch (FileNotFoundException e) {
			logger.error(e.toString(), e);
		} catch (IOException ex) {
			logger.error(ex.toString(), ex);
		} finally {
			try {
				in.close();
			} catch (Exception e) {
				logger.error(e.toString(), e);
			}
		}
		return props;
	}

}
