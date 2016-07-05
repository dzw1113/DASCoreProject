package com.icip.das.core.kafka;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducer {

	private final org.apache.kafka.clients.producer.KafkaProducer<Integer, String> producer;

	private final String topic;

	private final List<String> list;

	private Boolean isAsync = false;

	private Callback callBack;

	public Callback getCallBack() {
		return callBack;
	}

	public void setCallBack(Callback callBack) {
		this.callBack = callBack;
	}

	public KafkaProducer(String topic, List<String> list, Boolean isAsync) {
		producer = KafkaProducerFactory.getProducer();
		this.topic = topic;
		this.list = list;
		this.isAsync = isAsync;
	}

	public KafkaProducer(String topic, List<String> list) {
		this(topic, list, false);
	}

	public void send()throws Exception {
		for (int i = 0; i < list.size(); i++) {
			String redisKey = list.get(i);
			if (isAsync) {// FIXME -------->自增??
				asyncSend(i, redisKey);
			} else {
				syncSend(i, redisKey);
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

	private void syncSend(int messageNo, String message)throws Exception {
		try {
			producer.send(
					new ProducerRecord<Integer, String>(topic, messageNo,
							message)).get();
		} catch (InterruptedException e) {
			System.out.println(e);
			throw e;
		} catch (ExecutionException e) {
			System.out.println(e);
			throw e;
		}
	}

}
