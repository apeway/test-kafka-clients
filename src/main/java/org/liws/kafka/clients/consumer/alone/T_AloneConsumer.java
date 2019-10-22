package org.liws.kafka.clients.consumer.alone;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.liws.kafka.clients.KafkaProps;

/**
 * 独立consumer ：使用assign方法直接给consumer分配分区
 */
public class T_AloneConsumer {

	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProps());
		assignPartitionsToConsumer(consumer);
		
		try {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					consumeRecord(record);
				}
				consumer.commitSync();
			}
		} catch (WakeupException e) {
			// TODO: handle exception
		} finally {
			consumer.commitSync();
			consumer.close();
		}
	}

	private static Properties getProps() {
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG); 
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earkliest");
		return consumerProps;
	}

	private static void assignPartitionsToConsumer(KafkaConsumer<String, String> consumer) {
		List<TopicPartition> partitions = new ArrayList<>();
		String topic = "test-topic";
		List<PartitionInfo> allPartitions = consumer.partitionsFor(topic);
		if(allPartitions != null && !allPartitions.isEmpty()) {
			for (PartitionInfo partitionInfo : allPartitions) {
				partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
			}
		}
		consumer.assign(partitions); // XXX 使用assign()固定地为consumer指定其要消费的分区列表
	}
	
	/**
	 * 消费一条消息
	 * @param record
	 */
	private static void consumeRecord(ConsumerRecord<String, String> record) {
		System.out.println(String.format("topic=%s, partition=%d, offset=%d", 
				record.topic(), record.partition(), record.offset()));
	}
}
