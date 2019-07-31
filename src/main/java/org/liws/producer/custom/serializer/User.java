package org.liws.producer.custom.serializer;

import java.io.Serializable;

public class User implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private String userName;
	private String userCaption;
	private int age;
	
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getUserCaption() {
		return userCaption;
	}
	public void setUserCaption(String userCaption) {
		this.userCaption = userCaption;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}

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
