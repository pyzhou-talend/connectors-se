package org.talend.components.netsuite.runtime.avro.converter;

import org.apache.avro.Schema;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Unconverted<T> implements Converter<T, T> {

    private final Class<T> clazz;

    private final Schema schema;

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Class<T> getDatumClass() {
        return clazz;
    }

    @Override
    public T convertToDatum(T value) {
        return value;
    }

    @Override
    public T convertToAvro(T value) {
        return value;
    }

}
