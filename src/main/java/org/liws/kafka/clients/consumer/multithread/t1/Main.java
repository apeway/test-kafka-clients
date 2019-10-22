package org.liws.kafka.clients.consumer.multithread.t1;

import org.liws.kafka.clients.KafkaProps;

public class Main {
	public static void main(String[] args) {
		String brokerList = KafkaProps.BOOTSTRAP_SERVERS_CONFIG;
		String groupId = "";
		String topic = "";
		int consumerNum = 3;
		new ConsumerGroup(consumerNum, groupId, topic, brokerList);
	}
}
