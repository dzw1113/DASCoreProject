package com.icip.das.core.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.alibaba.fastjson.JSONObject;

public class MapKafkaProducer {

	private final org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;

	private final String topic;

	private final List<Map<String, String>> list;

	private Boolean isAsync = false;

	private Callback callBack;

	public Callback getCallBack() {
		return callBack;
	}

	public void setCallBack(Callback callBack) {
		this.callBack = callBack;
	}

	public MapKafkaProducer(String topic, List<Map<String, String>> list,
			Boolean isAsync) {
		producer = KafkaProducerFactory.getProducer();
		this.topic = topic;
		this.list = list;
		this.isAsync = isAsync;
	}

	public MapKafkaProducer(String topic, List<Map<String, String>> list) {
		this(topic, list, false);
	}

	public void send() {
		for (int i = 0; i < list.size(); i++) {
			Map<String, String> map = list.get(i);
			String str = JSONObject.toJSONString(map);
			// Map m = JSONObject.parseObject(str, Map.class);
			// System.err.println(m);
			if (isAsync) {// FIXME -------->自增??
				asyncSend(i, str);
			} else {
				syncSend(i, str);
			}
		}
	}

	private void asyncSend(int messageNo, String message) {
		this.setCallBack(new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata,
					Exception exception) {
				// 现在这个配置不会生效,空实现了yzk
			}
		});
		producer.send(new ProducerRecord<Integer, String>(topic, messageNo,
				message), this.getCallBack());
	}

	private void syncSend(int messageNo, String message) {
		try {
			producer.send(
					new ProducerRecord<Integer, String>(topic, messageNo,
							message)).get();
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch (ExecutionException e) {
			System.out.println(e);
		}
	}

}
