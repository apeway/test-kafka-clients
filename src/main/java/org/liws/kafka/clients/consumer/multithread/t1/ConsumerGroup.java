package org.liws.kafka.clients.consumer.multithread.t1;

/**
 * 消费线程管理类，创建多个线程类执行消费任务。
 */
public class ConsumerGroup {
	public ConsumerGroup(int consumerNum, String groupId, String topic, String brokerList) {
		for (int i = 0; i < consumerNum; i++) {
			new Thread(new ConsumerRunnable(brokerList, groupId, topic)).start();
		}
	}
}
