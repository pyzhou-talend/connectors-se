package org.talend.components.rabbitmq.exception;

public class ExchangeDeleteException extends RuntimeException {

    public ExchangeDeleteException(String message, Throwable cause) {
        super(message, cause);
    }
}
