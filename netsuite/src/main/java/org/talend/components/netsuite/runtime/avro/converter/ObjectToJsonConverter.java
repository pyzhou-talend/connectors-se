package org.talend.components.netsuite.runtime.avro.converter;

import java.io.IOException;

import org.apache.avro.Schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * Responsible for conversion of NetSuite <code>object</code> from/to <code>JSON</code>.
 */
public class ObjectToJsonConverter<T> implements Converter<T, String> {

    private Class<T> clazz;

    private ObjectReader objectReader;

    private ObjectWriter objectWriter;

    public ObjectToJsonConverter(Class<T> clazz, ObjectMapper objectMapper) {
        this.clazz = clazz;

        objectWriter = objectMapper.writer().forType(clazz);
        objectReader = objectMapper.reader().forType(clazz);
    }

    @Override
    public Schema getSchema() {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Class<T> getDatumClass() {
        return clazz;
    }

    @Override
    public String convertToAvro(T value) {
        if (value == null) {
            return null;
        }
        try {
            return objectWriter.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException();
            // TODO: Fix Exception
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), e,
            // ExceptionContext.build().put(ExceptionContext.KEY_MESSAGE, NetSuiteRuntimeI18n.MESSAGES
            // .getMessage("error.failedToConvertValueToJson", e.getMessage())));
        }
    }

    @Override
    public T convertToDatum(String value) {
        if (value == null) {
            return null;
        }
        try {
            return objectReader.readValue(value);
        } catch (IOException e) {
            throw new RuntimeException();
            // TODO: Fix Exception
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), e,
            // ExceptionContext.build().put(ExceptionContext.KEY_MESSAGE, NetSuiteRuntimeI18n.MESSAGES
            // .getMessage("error.failedToConvertValueFromJson", e.getMessage())));
        }
    }
}
