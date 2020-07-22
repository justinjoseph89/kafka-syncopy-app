package com.kafka.copy.app.exceptions;

public class TopicCreationException extends RuntimeException {

  private static final long serialVersionUID = 4707723157220283940L;

  public TopicCreationException(String errorMessage, Throwable err) {
    super(errorMessage, err);
  }
}