// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.rabbitmq.source;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.rabbitmq.exception.ExchangeDeclareException;
import org.talend.components.rabbitmq.exception.QueueDeclareException;
import org.talend.components.rabbitmq.service.I18nMessage;
import org.talend.components.rabbitmq.service.RabbitMQService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;

@Slf4j
@Documentation("Main class for JMSInput records processing")
public class InputSource implements Serializable {

    private final InputMapperConfiguration configuration;

    private final RabbitMQService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private int counter = 0;

    private Connection connection;

    private Channel channel;

    private String exchangeQueueName;

    private final I18nMessage i18n;

    public InputSource(@Option final InputMapperConfiguration configuration, final RabbitMQService service,
            final JsonBuilderFactory jsonBuilderFactory, final I18nMessage i18nMessage) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() throws IOException, TimeoutException, NoSuchAlgorithmException {
        connection = service.getConnection(configuration.getBasicConfig().getConnection());
        channel = service.createChannel(connection);

        switch (configuration.getBasicConfig().getReceiverType()) {
        case QUEUE:
            exchangeQueueName = configuration.getBasicConfig().getQueue();
            try {
                channel.queueDeclare(configuration.getBasicConfig().getQueue(), configuration.getBasicConfig().getDurable(),
                        false, configuration.getBasicConfig().getAutoDelete(), null);
            } catch (IOException e) {
                throw new QueueDeclareException(i18n.errorCantDeclareQueue(), e);
            }
            break;
        case EXCHANGE:
            try {
                channel.exchangeDeclare(configuration.getBasicConfig().getExchange(),
                        configuration.getBasicConfig().getExchangeType().getType());
                exchangeQueueName = channel.queueDeclare().getQueue();
                channel.queueBind(exchangeQueueName, configuration.getBasicConfig().getExchange(),
                        configuration.getBasicConfig().getRoutingKey());
            } catch (IOException e) {
                throw new ExchangeDeclareException(i18n.errorCantDeclareExchange(), e);
            }
            break;
        }
    }

    @Producer
    public JsonObject next() throws IOException {
        final String[] textMessage = { null };
        do {
            GetResponse response = channel.basicGet(exchangeQueueName, true);
            if (response != null) {
                textMessage[0] = new String(response.getBody());
            }
        } while (textMessage[0] == null && counter < configuration.getMaximumMessages());

        return textMessage[0] != null ? buildJSON(textMessage[0]) : null;
    }

    private JsonObject buildJSON(String text) {
        JsonObjectBuilder recordBuilder = jsonBuilderFactory.createObjectBuilder();
        recordBuilder.add(MESSAGE_CONTENT, text);
        counter++;
        return recordBuilder.build();
    }

    @PreDestroy
    public void release() {
        service.closeConnection(connection);
    }
}