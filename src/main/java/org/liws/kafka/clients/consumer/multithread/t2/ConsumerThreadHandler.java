package org.liws.kafka.clients.consumer.multithread.t2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * consumer多线程管理类，用于创建线程池以及为每个线程分配消息集合。
 * 另外consumer位移提交也在该类中完成。
 * @param <K>
 * @param <V>
 */
public class ConsumerThreadHandler<K, V> {

	private final KafkaConsumer<K, V> consumer;
	
	private ExecutorService executors;
	
	private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
	
	public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
		consumer = new KafkaConsumer<>(getProps(brokerList, groupId));
		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
			@Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				consumer.commitSync(offsets);	// 提交位移
			}
			@Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				offsets.clear();
			}
		}); 
	}
	private Properties getProps(String brokerList, String groupId) {
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList); 
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); 
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); 
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()); 
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earkliest");
		return consumerProps;
	}
	
	/**
	 * 消费主方法
	 */
	public void consume() {
		int threadNumber = Runtime.getRuntime().availableProcessors(); // cpuCount
		executors = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
		try {
			while(true) {
				ConsumerRecords<K, V> records = consumer.poll(1000);
				if(!records.isEmpty()) {
					executors.submit(new ConsumerWorker<>(records, offsets));
				}
				commitOffsets();
			}
		} catch (WakeupException e) {
			// TODO: handle exception
		} finally {
			commitOffsets();
			consumer.close();
		}
	}

	private void commitOffsets() {
		// 尽量降低synchronized块对offsets的锁定时间
		Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;
		synchronized (offsets) {
			if(offsets.isEmpty()) {
				return;
			}
			unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
			offsets.clear();
		}
		consumer.commitSync(unmodfiedMap);
	}
	
	public void close() {
		consumer.wakeup();
		executors.shutdown();
	}
}
