package com.icip.das.core.task;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.jdbc.DataInitLoader;

/** 
 * @Description: 定时(每分钟)重新加载rolling_config，rolling_config_custom两表配置
 * @author  
 * @date 2016年3月21日 上午10:49:58 
 * @update	
 */
public class ReloadTask extends TimerTask{

	private static final Logger logger = LoggerFactory.getLogger(ReloadTask.class);

	public void run() {
		try {
			reloadTableConfig();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	private void reloadTableConfig() {
		logger.info("ReloadTask-------定时重载tableConfig -------->");
		DataInitLoader.TABLE_CONFIG.clear();
		DataInitLoader.getTableConfig();
	}
	
}
