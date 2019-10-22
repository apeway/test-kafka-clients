package org.liws.kafka.clients.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.liws.kafka.clients.KafkaProps;

public class T1_Producer {

	public static void main(String[] args) {
		
		/*Producer<String, String> producer = new KafkaProducer<>(getProps());*/
		Producer<String, String> producer = new KafkaProducer<>(getProps(), 
				new StringSerializer(), new StringSerializer());
		
		String testTopic = "test-java-topic";
		String testMsgVal = "msg test to test-java-topic";
		// record中可以不用指定分区和key，由kafka自行确定目标分区
		ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, testMsgVal);
		
		sendRecord(producer, record);
		
		producer.close(); // 别忘了close！
	}

	/**
	 * 三种发送消息方式
	 * @param producer
	 * @param record
	 */
	private static void sendRecord(Producer<String, String> producer, ProducerRecord<String, String> record) {
		// 发送方式1、fire and forget ,即发送后不理会发送结果【不推荐】
		producer.send(record);

		// 发送方式2、默认的异步发送 + 回调
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception == null) {
					// 消息发送成功
				} else {
					if(exception instanceof RetriableException) {
						// 处理可重试瞬时异常
					} else {
						// 处理不可重试异常
					}
					exception.printStackTrace();
				}
			}
		});

		// 发送方式3、同步发送，通过Future.get()无限等待结果返回即实现了同步发送的效果
		try {
			producer.send(record).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Properties getProps() {
		Properties props = new Properties();
		// bootstrap.servers、key.serializer、value.serializer没有默认值，必须指定
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
		/*// key.serializer、value.serializer可在Producer构造方法中指定
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());*/
		
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		return props;
	}
}
