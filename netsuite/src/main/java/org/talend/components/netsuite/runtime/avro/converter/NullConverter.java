package org.talend.components.netsuite.runtime.avro.converter;

import org.apache.avro.Schema;

import lombok.AllArgsConstructor;

/**
 * Special converter which converts any value to {@code null}.
 */
@AllArgsConstructor
public class NullConverter<T> implements Converter<T, T> {

    private final Class<T> clazz;

    private final Schema schema;

    @Override
    public T convertToAvro(T value) {
        return null;
    }

    @Override
    public T convertToDatum(T value) {
        return null;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Class<T> getDatumClass() {
        return clazz;
    }
}
