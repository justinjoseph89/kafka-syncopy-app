package com.kafka.copy.app.exceptions;

public class TopicNotAvailableException extends Exception {

	private static final long serialVersionUID = 4707723157220283940L;

	public TopicNotAvailableException(String errorMessage) {
		super(errorMessage);
	}
}