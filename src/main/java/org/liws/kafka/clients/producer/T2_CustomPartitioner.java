package org.liws.kafka.clients.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.liws.kafka.clients.KafkaProps;
import org.liws.kafka.clients.custom.partitioner.CustomPartitioner;

/**
 * 测试自定义分区策略
 */
public class T2_CustomPartitioner {

	public static void main(String[] args) throws Exception {
		
		Producer<String, String> producer = new KafkaProducer<>(getProps());
		
		String topic = "test-custom-topic";
		ProducerRecord<String, String> nonekeyRecord = new ProducerRecord<>(topic, "没有key的消息");
		ProducerRecord<String, String> auditRecord = new ProducerRecord<>(topic, "audit_key1", "一条审计消息");
		ProducerRecord<String, String> noneAuditRecord = new ProducerRecord<>(topic, "none_audit_key1", "一条非审计消息");
		
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
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		// XXX 自定义分区策略
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
		return props;
	}
}
