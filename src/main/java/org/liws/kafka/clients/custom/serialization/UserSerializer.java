package org.liws.kafka.clients.custom.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

public class UserSerializer implements Serializer<User> {

	private ObjectMapper objMapper;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		objMapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(String topic, User data) {
		byte[] result = null;
		try {
			result = objMapper.writeValueAsString(data).getBytes("UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public void close() {
	}

}
