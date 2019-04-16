package org.talend.components.rabbitmq.exception;

public class QueueDeleteException extends RuntimeException {

    public QueueDeleteException(String message, Throwable cause) {
        super(message, cause);
    }
}
