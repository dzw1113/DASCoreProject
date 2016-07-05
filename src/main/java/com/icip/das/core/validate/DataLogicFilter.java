package com.icip.das.core.validate;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.icip.das.core.data.FilterBean;
import com.icip.das.core.data.TableConfigBean;
import com.icip.das.core.exception.SysException;
import com.icip.das.core.jdbc.QueryFilterConfig;
import com.icip.das.core.thread.WorkerPoolManager.WorkerProxyObject;

/**
 * @Description: 没完成
 * @author yzk
 * @date 2016年2月29日 下午11:14:20
 * @update
 */
public class DataLogicFilter implements IDataFilter<WorkerProxyObject> {

	/** 分割标志'|' */
	private static final String FLAG_COMMA = "\\|";

	private static final String RULE_AND = "0";
	private static final String RULE_OR = "1";

	@Override
	@SuppressWarnings("unchecked")
	public WorkerProxyObject handle(WorkerProxyObject proxy)
			throws SysException {
		Map<String, FilterBean> configs = QueryFilterConfig.getConfig((TableConfigBean) proxy.getConfig());
		if (null == configs || configs.isEmpty())
			return proxy;

		List<? extends Map<String,String>> dataList = (List<? extends Map<String, String>>) proxy.getData();
		for (Map.Entry<String, FilterBean> entry : configs.entrySet()) {
			String columnName = entry.getKey();
			FilterBean config = entry.getValue();
			
			Iterator<Map<String,String>> iterator = (Iterator<Map<String, String>>) dataList.iterator();//ConcurrentModificationException
			while(iterator.hasNext()){
				Map<String, String> data = iterator.next();
				boolean isRemove = false;
				if (RULE_AND.equals(config.getLogic())) {// 逻辑AND
					isRemove = logicJudgeAnd(columnName, config, data);
				} else if (RULE_OR.equals(config.getLogic())) {
					isRemove = logicJudgeOr(columnName, config, data);
				}
				if(isRemove)
					iterator.remove();
			}
		}
		return proxy;
	}

	private boolean logicJudgeOr(String columnName, FilterBean config,
			Map<String, String> data) {
		String columnValue = data.get(columnName);

		int flag = 0;// 判断标志

		if (StringUtils.isBlank(columnValue)) {//空值认为合法
			flag++;
		}
		if (!StringUtils.isBlank(config.getRegex())) {
			boolean regu = regex(columnValue, config.getRegex());
			if (regu == true)
				flag++;
		} 
		if (!StringUtils.isBlank(config.getRange())) {
			boolean ran = range(columnValue, config.getRange());
			if (ran == true)
				flag++;
		} 
		if (!StringUtils.isBlank(config.getRegular())) {
			boolean reg = regular(columnValue, config.getRegular());
			if (reg == true)
				flag++;
		}
		if (!StringUtils.isBlank(config.getMatch())) {
			boolean mat = match(columnValue, config.getMatch());
			if (mat == true)
				flag++;
		}

		if(flag != 0) {
			return true;
		}else{
			return false;
		}
	}

	private boolean logicJudgeAnd(String columnName, FilterBean config,
			Map<String, String> data) {
		String columnValue = data.get(columnName);

		String regex = config.getRegex();
		String regular = config.getRegular();
		String match = config.getMatch();
		String range = config.getRange();

		int flag_init = 0;
		if(!StringUtils.isBlank(regex))
			flag_init++;
		if(!StringUtils.isBlank(regular))
			flag_init++;
		if(!StringUtils.isBlank(match))
			flag_init++;
		if(!StringUtils.isBlank(range))
			flag_init++;
		
		int flag = 0;// 判断标志

		if (null == columnValue) {//空值认为合法
			flag = flag_init;
		}
		
		if (!StringUtils.isBlank(regex)) {
			// 正则匹配方法regular
			boolean regu = regex(columnValue, config.getRegex());
			if (regu == true)
				flag++;
		}
		if (!StringUtils.isBlank(range)) {
			// 调用枚举匹配方法range
			boolean ran = range(columnValue, config.getRange());
			if (ran == true)
				flag++;
		}
		if (!StringUtils.isBlank(regular)) {
			// 调用常规模糊匹配方法regular
			boolean reg = regular(columnValue, config.getRegular());
			if (reg == true)
				flag++;
		}
		if (!StringUtils.isBlank(match)) {
			// 调用常规完全匹配方法match
			boolean mat = match(columnValue, config.getMatch());
			if (mat == true)
				flag++;
		}

		if (flag == flag_init) {
			return true;
		} else {
			return false;
		}
	}

	// 正则匹配判断
	private boolean regex(String param, String value) {
		Pattern p = Pattern.compile(value);
		Matcher m = p.matcher(param);
		boolean result = m.matches();
		
		if (result == true) {
			return true;
		} else {
			return false;
		}
	}

	// 合法值枚举匹配
	private boolean range(String param, String value) {
		String[] values = value.split(FLAG_COMMA);
		List<String> legalValues = Arrays.asList(values);
		if (legalValues.contains(param))
			return true;

		return false;
	}

	// 模糊匹配
	private boolean regular(String param, String value) {
		if (param.contains(value) && !param.equals(value)) {
			return true;
		} else {
			return false;
		}
	}

	// 完全匹配
	private boolean match(String param, String value) {
		if (param.equals(value)) {
			return true;
		} else {
			return false;
		}
	}

}
