package org.liws.producer.custom.partitioner;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class T2_CustomPartitioner {

	public static void main(String[] args) throws Exception {
		
		Properties props = getProps();

		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		
		String topic = "test-custom-topic";
		ProducerRecord<String, String> nonekeyRecord = new ProducerRecord<>(topic, "none-key record");
		ProducerRecord<String, String> auditRecord = new ProducerRecord<>(topic, "auditkey1", "audit record");
		ProducerRecord<String, String> noneAuditRecord = new ProducerRecord<>(topic, "otherkey1", "none-audit record");
		
		for (int i = 0; i < 3; i++) {
			producer.send(auditRecord).get();
		}
		for (int i = 0; i < 10; i++) {
			producer.send(nonekeyRecord).get();
			producer.send(noneAuditRecord).get();
		}
		
		producer.close();
		
		
	}

	private static Properties getProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.6.233.62:9093,10.6.233.62:9094,10.6.233.62:9095");
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		// 自定义分区策略
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.liws.producer.custom.partitioner.CustomPartitioner");
		return props;
	}
}
