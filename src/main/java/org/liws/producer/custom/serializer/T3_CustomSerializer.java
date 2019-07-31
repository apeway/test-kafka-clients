package org.liws.producer.custom.serializer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class T3_CustomSerializer {

	public static void main(String[] args) throws Exception {
		
		Properties props = getProps();

		Producer<String, User> producer = new KafkaProducer<>(props);

		String topic = "test-java-topic";
		ProducerRecord<String, User> record = new ProducerRecord<>(topic, new User("zhangsan", "张三", 23));

		producer.send(record).get();
		
		producer.close();
	}

	private static Properties getProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.6.233.62:9093,10.6.233.62:9094,10.6.233.62:9095");
		// XXX 自定义序列化必须在这里配置，在Producer构造器中指定报错
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.liws.producer.custom.serializer.UserSerializer");
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		return props;
	}
}
