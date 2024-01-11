package com.bss.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.bss.bean.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ConsumerService {
	
	
	@KafkaListener(topics = "topicDemo", groupId = "console-consumer")
	public void readMessages(String message) {
		if (isJsonMessage(message)) {
	        // It's a JSON-formatted message
	        try {
	            Message myMessage = convertJsonToMessage(message);
	            // Your logic for handling the Message instance
	            System.out.println("Received Message: " + myMessage);
	        } catch (JsonProcessingException e) {
	            System.out.println("Failed to convert JSON to Message object. Reason: " + e.getMessage());
	            System.out.println("Received Plain String: " + message);
	            // Handle accordingly
	        }
	    } else {
	        // It's a regular string message
	        System.out.println("Received Plain String: " + message);
	        // Handle accordingly
	    }
	}
	
	
	private boolean isJsonMessage(String message) {
		
		return (message !=null && message.startsWith("{") &&  message.endsWith("}") );
		
	}

	
	private Message convertJsonToMessage(String message) throws JsonMappingException, JsonProcessingException {
		ObjectMapper obj = new ObjectMapper();
		return obj.readValue(message, Message.class);
	}

}
