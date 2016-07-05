package com.icip.framework.aggregate;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import storm.trident.tuple.TridentTuple;

/**
 * 
 * @Description: sum聚合器
 * @author
 * @date 2016年3月19日 上午12:29:50
 * @update
 */
public class SumAggregatorImpl extends AbstractAggregator {

	public SumAggregatorImpl(String fieldKey) {
		super(fieldKey);
	}

	private static final long serialVersionUID = -7034885364836795070L;

	@SuppressWarnings("unchecked")
	@Override
	public Double init(TridentTuple tuple) {
		Map<String, String> map = (Map<String, String>) tuple.get(0);
		Float number = Float
				.parseFloat(StringUtils.isEmpty(map.get(clacKey)) ? "0" : map
						.get(clacKey));
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
