package com.bss.bean;


public class Message {
	
	
	private Integer id;
	private String message;
	private String date;
	private String priority;
	private Boolean ststus;
	public Message(Integer id, String message, String date, String priority, Boolean ststus) {
		super();
		this.id = id;
		this.message = message;
		this.date = date;
		this.priority = priority;
		this.ststus = ststus;
	}
	public Message() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getPriority() {
		return priority;
	}
	public void setPriority(String priority) {
		this.priority = priority;
	}
	public Boolean getStstus() {
		return ststus;
	}
	public void setStstus(Boolean ststus) {
		this.ststus = ststus;
	}
	@Override
	public String toString() {
		return "Message [id=" + id + ", message=" + message + ", date=" + date + ", priority=" + priority + ", ststus="
				+ ststus + "]";
	}
	
	
	
	

}
