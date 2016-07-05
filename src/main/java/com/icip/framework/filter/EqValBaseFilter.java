package com.icip.framework.filter;

import java.util.Map;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @Description: 根据key去找value，做eq比较
 * @author
 * @date 2016年3月28日 下午7:37:48
 * @update
 */
public class EqValBaseFilter extends BaseFilter {

	private static final long serialVersionUID = -2915480589935504771L;
	protected String key;
	protected String[] values;

	public EqValBaseFilter(String key, String... values) {
		this.key = key;
		this.values = values;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean isKeep(TridentTuple tuple) {
		boolean flag = false;
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		String val = map.get(key);
		for (int i = 0; i < values.length; i++) {
			if (values[i].equals(val)) {
				flag = true;
				break;
			}
		}
		return flag;
	}

}
