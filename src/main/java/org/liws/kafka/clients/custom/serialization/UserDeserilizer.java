package org.liws.kafka.clients.custom.serialization;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

public class UserDeserilizer implements Deserializer<User> {

	private ObjectMapper objMapper;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		objMapper = new ObjectMapper();
	}

	@Override
	public User deserialize(String topic, byte[] data) {
		User user = null;
		try {
			user = objMapper.readValue(data, User.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return user;
	}

	@Override public void close() {}
}
