package org.liws.kafka.clients.consumer.multithread.t2;

import org.liws.kafka.clients.KafkaProps;

public class Main {

	public static void main(String[] args) {
		String brokerList = KafkaProps.BOOTSTRAP_SERVERS_CONFIG;
		String groupId = "";
		String topic = "";
		final ConsumerThreadHandler<byte[], byte[]> handler = 
				new ConsumerThreadHandler<>(brokerList, groupId, topic);
		new Thread(new Runnable() {
			@Override public void run() {
				handler.consume();
			}
		}).start();
		
		// 20s后自动停止该测试程序
		try {
			Thread.sleep(20000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("关闭consumer......");
		handler.close();
	}
}
