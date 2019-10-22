package org.liws.kafka.clients.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.liws.kafka.clients.KafkaProps;

public class T1_Consumer {

	public static void main(String[] args) {
		// 创建KafkaConsumer实例
		// KafkaConsumer<String, String> consumer = new KafkaConsumer<>(genConsumerProps());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(genConsumerProps(), new StringDeserializer(), new StringDeserializer());
		
		// 订阅topic列表
		consumer.subscribe(Arrays.asList("test-java-topic"));

try {
	// 循环调用poll方法获取封装在ConsumerRecord中的topic消息并处理
	while (true) {
		ConsumerRecords<String, String> records = consumer.poll(1000);
		for (ConsumerRecord<String, String> record : records) {
			System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
					record.value());
		}
	}
} catch(WakeupException e) {
	// 异常处理
} finally {
	// 关闭KafkaConsumer
	consumer.close();
}
	}

	private static Properties genConsumerProps() {
		Properties consumerProps = new Properties();
		// bootstrap.servers、group.id、key.serializer、value.serializer没有默认值,必须指定
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG); 
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group"); 
		/*// key.deserializer、value.deserializer可在KafkaConsumer构造方法中指定
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); 
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());*/
		
		// 其它属性
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // 从最早的消息开始读取
		return consumerProps;
	}
	
}
