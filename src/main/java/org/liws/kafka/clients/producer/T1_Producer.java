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

/**
 * KafkaProducer
 * ProducerRecord : 待发送消息对象
 * RecordMetadata : 已发送消息的所有元数据信息，包括消息发送的topic、分区以及该消息在对应分区的位移信息等。
 */
public class T1_Producer {

	public static void main(String[] args) {
		
		/*Producer<String, String> producer = new KafkaProducer<>(getProps());*/
		Producer<String, String> producer = new KafkaProducer<>(getProps(), 
				new StringSerializer(), new StringSerializer());
		
		String testTopic = "topiclws";
		String testMsgVal = "msg test to test-java-topic";
		// ProducerRecord中可以不用指定分区和key，由kafka自行确定目标分区
		ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, testMsgVal);
		
		// 三种发送消息方式
		send(producer, record);
		sendSync(producer, record);
		sendAsync(producer, record);
		
		producer.close(); // 别忘了close！
	}

	private static void send(Producer<String, String> producer, ProducerRecord<String, String> record) {
		// 发送方式1、fire and forget ,即发送后不理会发送结果【实际中不推荐】
		producer.send(record);
	}
	/**
	 * XXX 发送方式2、【同步发送】，
	 * 	   实际上"Future<RecordMetadata> send(ProducerRecord<K, V> record)"默认也是异步的，返回的Future供
	 * 用户稍后获取发送结果，这就是所谓的回调机制。
	 * 	   但是它同样也能实现同步发送的效果， 这里的同步发送和异步发送是通过Future来区分的，调用Future.get()
	 * 无限等待结果返回即实现了同步发送的效果。
	 */
	private static void sendSync(Producer<String, String> producer, ProducerRecord<String, String> record) {
		try {
			/*
			 *   使用Future.get()会一直等待下去直至Kafka broker将发送结果返回给producer程序。当结果从broker
			 * 处返回时，get()要么返回RecordMetadata实例要么抛出异常交由producer自行处理。
			 */
			@SuppressWarnings("unused")
			RecordMetadata recordMetadata = producer.send(record).get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * XXX 发送方式3、【异步发送 + 回调】
	 */
	private static void sendAsync(Producer<String, String> producer, ProducerRecord<String, String> record) {
		producer.send(record, new Callback() {
			/**
			 * metadata和exception两个参数至少有一个是null。
			 * 当消息发送成功时exception为null，反之则metadata为null。
			 */
			@Override public void onCompletion(RecordMetadata metadata, Exception exception) {
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
	}
	
	private static Properties getProps() {
		Properties props = new Properties();
		/* XXX bootstrap.servers、key.serializer、value.serializer没有默认值，必须指定
		 * 另外key.serializer、value.serializer可在KafkaProducer的构造方法中指定 */
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
		/* props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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
