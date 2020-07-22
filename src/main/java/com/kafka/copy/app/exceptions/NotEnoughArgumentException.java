package com.kafka.copy.app.exceptions;

public class NotEnoughArgumentException extends Exception {

	private static final long serialVersionUID = 4707723157220283940L;

	public NotEnoughArgumentException(String errorMessage) {
		super(errorMessage);
	}
}