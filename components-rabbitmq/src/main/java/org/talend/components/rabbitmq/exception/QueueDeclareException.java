package org.talend.components.rabbitmq.exception;

public class QueueDeclareException extends RuntimeException {

    public QueueDeclareException(String message, Throwable cause) {
        super(message, cause);
    }
}
