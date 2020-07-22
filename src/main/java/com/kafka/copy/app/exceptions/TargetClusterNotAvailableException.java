package com.kafka.copy.app.exceptions;

public class TargetClusterNotAvailableException extends Exception {

	private static final long serialVersionUID = 4707723157220283940L;

	public TargetClusterNotAvailableException(String errorMessage) {
		super(errorMessage);
	}
}