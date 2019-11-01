package org.liws.kafka.clients;

/**
 * 配置
 */
public interface KafkaProps {
	String ip = "10.6.215.45";
	String BOOTSTRAP_SERVERS_CONFIG = ip + ":9093," + ip + ":9094," + ip + ":9095";
}
