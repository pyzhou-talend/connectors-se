package org.talend.components.rabbitmq.publisher;

import java.io.IOException;

public interface MessagePublisher {

    void publish(String message) throws IOException;
}