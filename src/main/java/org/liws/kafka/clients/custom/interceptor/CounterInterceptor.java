package org.liws.kafka.clients.custom.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 该intcrceptor会在消息发送后更新“发送成功消息数”和“发送失败消息数”两个计数器，
 * 并在producer关闭时打印这两个计数器。
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

	private int errCounter; 
	private int succCounter;
	
	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		if(exception == null) {
			succCounter++;
		} else {
			errCounter++;
		}
	}

	@Override
	public void close() {
		System.out.println("发送成功消息数" + succCounter);
		System.out.println("发送失败消息数" + errCounter);
	}

}
