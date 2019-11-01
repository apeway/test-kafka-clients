package org.liws.kafka.clients.producer;

import java.util.Arrays;
import java.util.List;
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
 * 测试interceptor
 */
public class T4_CustomInterceptor {

	public static void main(String[] args) throws Exception {

		Producer<String, String> producer = new KafkaProducer<>(getProps(), 
				new StringSerializer(), new StringSerializer());

		String testTopic = "test-java-topic";
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, "msg" + i);
			producer.send(record).get();
		}
		
		// 一定要关闭producer，这样才会调用interceptor的close方法
		producer.close();
	}

	private static Properties getProps() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
		
		// XXX 拦截器链
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, getInterceptors());
		
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		return props;
	}

	private static List<String> getInterceptors() {
		return Arrays.asList("org.liws.kafka.clients.custom.interceptor.TimeStampPrependerInterceptor",
				"org.liws.kafka.clients.custom.interceptor.CounterInterceptor");
	}
	
}
