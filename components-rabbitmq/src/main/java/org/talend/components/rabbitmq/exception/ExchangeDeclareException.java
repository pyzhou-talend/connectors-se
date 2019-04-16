package org.talend.components.rabbitmq.exception;

public class ExchangeDeclareException extends RuntimeException {

    public ExchangeDeclareException(String message, Throwable cause) {
        super(message, cause);
    }
}
