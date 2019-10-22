package org.liws.kafka.clients.consumer.multithread.t1;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 消费线程类，执行真正的消费任务
 */
public class ConsumerRunnable implements Runnable {

	/** 每个消费线程维护一个私有的KafkaConsumer实例 */
	private final KafkaConsumer<String, String> consumer;
	
	public ConsumerRunnable(String brokerList, String groupId, String topic) {
		consumer = new KafkaConsumer<>(getProps(brokerList, groupId));
		consumer.subscribe(Arrays.asList(topic)); // 使用分区副本自动分配策略
	}
	private Properties getProps(String brokerList, String groupId) {
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList); 
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); 
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); 
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 使用自动提交位移
		consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
		consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		return consumerProps;
	}
	
	@Override
	public void run() {
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
			for (ConsumerRecord<String, String> record : records) {
				consumeRecord(record);
			}
		}
	}
	private void consumeRecord(ConsumerRecord<String, String> record) {
		System.out.println(String.format("%s消费了一条消息, partition=%d, offset=%d, key=%s, value=%s", 
				Thread.currentThread().getName(), record.partition(), record.offset(), record.key(), record.value()));
	}
}
