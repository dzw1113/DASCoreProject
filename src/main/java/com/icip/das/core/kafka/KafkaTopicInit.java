package com.icip.das.core.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.jdbc.DataInitLoader;

/** 
 * @Description: TODO
 * @author  
 * @date 2016年4月23日 上午9:47:33 
 * @update	
 */
public class KafkaTopicInit {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaTopicInit.class);

	//只创建主题发送空消息
	public static void initTopic(){
		Set<Entry<String, TableConfigBean>> entrySet = DataInitLoader.getTableConfig().entrySet();
		for(Entry<String, TableConfigBean> entry : entrySet){
			List<String> list = new ArrayList<String>();
			list.add("");
			TableConfigBean config = entry.getValue();
			String handleFlag = config.getHandleFlag();
			String tableName = config.getTableName();
			if(!StringUtils.isBlank(handleFlag) && "1".equals(handleFlag)){
				KafkaProducer producer = new KafkaProducer(tableName,list);
				try{
					producer.send();
				}catch(Exception ex){
					logger.error(ex.toString(),ex);
				}
			}
		}
	}
	
}
