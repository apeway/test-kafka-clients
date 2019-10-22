package org.liws.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.liws.kafka.clients.KafkaProps;

public class T1_Consumer2 {

	public static void main(String[] args) {
Properties consumerProps = new Properties();
consumerProps.put("bootstrap.servers", "localhost:9092");
consumerProps.put("group.id", "test-group");
consumerProps.put("enable.auto.commit", false);

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
		new StringDeserializer());

// 订阅topic列表
consumer.subscribe(Arrays.asList("test-java-topic"));

// 定义一个缓冲区，然后按批获取消息并将它们加入缓冲区中。
// 当积累了足够多的消息时便统一插入到数据库中，然后手动提交位移并清空缓冲区以备缓存下一批消息。

boolean running = true;

try {
	while (running) {
		ConsumerRecords<String, String> records = consumer.poll(1000);
		// 对poll方法返回的消息集合按分区进行分组，然后按分区进行位移提交
		for (TopicPartition partition : records.partitions()) {
			List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
			for (ConsumerRecord<String, String> record : partitionRecords) {
				System.out.println(record.offset() + ": " + record.value());
			}
			long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
			// 注意：提交的位移一定是下一条待读取消息的位移
			consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
		}
	}

} finally {
	consumer.close();
}

	}

}
