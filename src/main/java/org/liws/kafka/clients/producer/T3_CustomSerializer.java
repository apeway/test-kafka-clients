package org.liws.kafka.clients.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.liws.kafka.clients.KafkaProps;
import org.liws.kafka.clients.custom.serialization.User;
import org.liws.kafka.clients.custom.serialization.UserSerializer;

/**
 * 测试自定义serializer
 */
public class T3_CustomSerializer {

	public static void main(String[] args) throws Exception {

		Producer<String, User> producer = new KafkaProducer<>(getProps());

		String testTopic = "topiclws";
		User testMsgVal = new User("zhangsan", "张三", 23);
		ProducerRecord<String, User> record = new ProducerRecord<>(testTopic, testMsgVal);

		producer.send(record).get();
		
		producer.close();
	}

	private static Properties getProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
		// XXX 自定义序列化必须在这里配置，在Producer构造器中指定报错
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());
		
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		return props;
	}
}
