package org.liws.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringSerializer;

public class T1_Producer {

	public static void main(String[] args) {
		
		Properties props = getProps();

		/*Producer<String, String> producer = new KafkaProducer<>(props);*/
		Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		
		// record中可以不用指定分区和key，由kafka自行确定目标分区
		String topic = "test-java-topic";
		String testmsg = "msg test to test-java-topic";
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, testmsg);
		
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
		
		// 别忘了close！
		producer.close();
	}

	private static Properties getProps() {
		Properties props = new Properties();
		// bootstrap.servers、key.serializer、value.serializer没有默认值，必须指定
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.6.233.62:9093,10.6.233.62:9094,10.6.233.62:9095");
		
		/*  // 这俩属性可在Producer构造方法中指定
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");*/
		props.put(ProducerConfig.ACKS_CONFIG, "-1");
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
		return props;
	}
}
