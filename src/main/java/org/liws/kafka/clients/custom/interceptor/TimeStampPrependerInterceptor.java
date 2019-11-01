package org.liws.kafka.clients.custom.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 此拦截器负责在消息体最前面追加时间戳
 */
public class TimeStampPrependerInterceptor implements ProducerInterceptor<String, String> {

	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		return new ProducerRecord<String, String>(record.topic(), record.partition(), record.timestamp()
				, record.key(), System.currentTimeMillis() + "," + record.value().toString());
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
	}

	@Override
	public void close() {
	}

}
