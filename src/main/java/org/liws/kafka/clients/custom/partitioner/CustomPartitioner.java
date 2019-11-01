package org.liws.kafka.clients.custom.partitioner;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

/**
 * 通过实现Partitioner接口来自定义分区器
 */
public class CustomPartitioner implements Partitioner {

	private Random ran = null;
	
	/**
	 * 该方法实现必要的资源初始化工作
	 */
	@Override
	public void configure(Map<String, ?> configs) {
		ran = new Random();
	}
	/**
	 * 该方法实现必要的资源清理工作
	 */
	@Override
	public void close() {
	}

	/**
	 * 实现自定义分区策略，计算消息要被发送到哪个分区
	 * @param topic 主题名称
	 * @param keyObject 消息键值（或null）
	 * @param keyBytes 消息键值序列化字节数组（或null）
	 * @param valueObject 消息体（或null）
	 * @param valueBytes 消息体序列化字节数组（或null）
	 * @param cluster 集群元数据
	 */
	@Override
	public int partition(String topic, Object keyObject, byte[] keyBytes, 
			Object valueObject, byte[] valueBytes, Cluster cluster) {
		String key = (String)keyObject;
		// 从集群元数据中读取指定topic的所有分区信息
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		
		/* 分区策略设计：
		 *  若消息key中包含"audit"【表明该消息用于审计功能】，则固定的发送到最后一个分区；
		 *  否则随机发送到除最后一个分区外的其它分区中。
		 *  XXX 另外，用户也可以根据value来定制化分区策略。
		 */
		int auditPartition = partitions.size() - 1;
		if (key != null && key.contains("audit")) { 
			return auditPartition;
		} else {
			return ran.nextInt(auditPartition);  
		}
	}

}
