package com.kafka.copy.app.exceptions;

public class SourceClusterNotAvailableException extends Exception {

	private static final long serialVersionUID = 4707723157220283940L;

	public SourceClusterNotAvailableException(String errorMessage) {
		super(errorMessage);
	}
}