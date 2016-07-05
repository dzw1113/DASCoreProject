package com.icip.framework.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

public abstract class AbstractRedisStateQueryFunction <T extends State> extends BaseQueryFunction<T, List<Values>> {
	
	private final RedisLookupMapper lookupMapper;
	private static final long serialVersionUID = 6286031231360286641L;
	protected final RedisDataTypeDescription.RedisDataType dataType;
	protected final String additionalKey;
	

	/**
	 * Constructor
	 *
	 * @param lookupMapper mapper for querying
	 */
	public AbstractRedisStateQueryFunction(RedisLookupMapper lookupMapper) {
		this.lookupMapper = lookupMapper;
		RedisDataTypeDescription dataTypeDescription = lookupMapper.getDataTypeDescription();
		this.dataType = dataTypeDescription.getDataType();
		this.additionalKey = dataTypeDescription.getAdditionalKey();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<List<Values>> batchRetrieve(T state, List<TridentTuple> inputs) {
		List<List<Values>> values = Lists.newArrayList();

		List<String> keys = Lists.newArrayList();
		
		for (TridentTuple input : inputs) {
			String []str = {};
			str = input.getValues().toArray(str);
			keys.addAll(Arrays.asList(str));
		}

		List<String> redisVals = retrieveValuesFromRedis(state, keys);
		Object objs[] = redisVals.toArray();
		List<Values> vals = new ArrayList<Values>();
		vals.add(new Values(objs));
		values.add(vals);

		return values;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void execute(TridentTuple tuple, List<Values> values, TridentCollector collector) {
		for (Values value : values) {
			collector.emit(value);
		}
	}

	/**
	 * Retrieves values from Redis that each value is corresponding to each key.
	 *
	 * @param state State for handling query
	 * @param keys keys having state values
	 * @return values which are corresponding to keys
	 */
	protected abstract List<String> retrieveValuesFromRedis(T state, List<String> keys);

}
