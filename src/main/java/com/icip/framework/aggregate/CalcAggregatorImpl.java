package com.icip.framework.aggregate;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import storm.trident.tuple.TridentTuple;

/**
 * 
 * @Description: 计算聚合器,fieldKey指定值和fieldVal预期值一样则做加法，其他则正常处理
 * @author
 * @date 2016年3月19日 上午12:29:50
 * @update
 */
public class CalcAggregatorImpl extends AbstractAggregator {

	public CalcAggregatorImpl(String clacKey, String fieldKey, String fieldVal) {
		super(clacKey, fieldKey, fieldVal);
	}

	private static final long serialVersionUID = -7034885364836795070L;

	@SuppressWarnings("unchecked")
	@Override
	public Double init(TridentTuple tuple) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Float number = Float.parseFloat("0");
		if (!StringUtils.isEmpty(map.get(clacKey))) {
			try {
				number = Float.parseFloat(map.get(clacKey));
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
		}
		if (map.get(fieldKey).equals(fieldVal)) {
			number = 0 - number;
		}

		BigDecimal decimalFormat = new BigDecimal(number);
		Double re = decimalFormat.setScale(4, BigDecimal.ROUND_HALF_UP)
				.doubleValue();
		return re;
	}

	@Override
	public Double combine(Double val1, Double val2) {
		BigDecimal decimalFormat = new BigDecimal(val1 + val2);
		Double re = decimalFormat.setScale(4, BigDecimal.ROUND_HALF_UP)
				.doubleValue();
		return re;
	}

	@Override
	public Double zero() {
		return Double.parseDouble("0");
	}

}
