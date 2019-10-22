package org.liws.kafka.clients.custom.serialization;

import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class User implements Serializable {
	private static final long serialVersionUID = 1L;

	private String userName;
	private String userCaption;
	private int age;

	public User(String userName, String userCaption, int age) {
		super();
		this.userName = userName;
		this.userCaption = userCaption;
		this.age = age;
	}

	@Override
	public String toString() {
		return "User [userName=" + userName + ", userCaption=" + userCaption + ", age=" + age + "]";
	}
}
