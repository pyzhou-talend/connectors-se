package org.talend.components.netsuite.runtime.avro.converter;

import org.apache.avro.Schema;
import org.talend.components.netsuite.runtime.model.beans.EnumAccessor;

import lombok.AllArgsConstructor;

/**
 * Responsible for conversion of NetSuite <code>Enum Constant</code> from/to <code>string</code>.
 */
@AllArgsConstructor
public class EnumToStringConverter<T extends Enum<T>> implements Converter<T, String> {

    private final Class<T> clazz;

    private final EnumAccessor enumAccessor;

    @Override
    public Schema getSchema() {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Class<T> getDatumClass() {
        return clazz;
    }

    @Override
    public T convertToDatum(String value) {
        if (value == null) {
            return null;
        }
        try {
            return (T) enumAccessor.getEnumValue(value);
        } catch (IllegalArgumentException ex) {
            // Fallback to .valueOf(String)
            return Enum.valueOf(clazz, value);
        }
    }

    @Override
    public String convertToAvro(T enumValue) {
        if (enumValue == null) {
            return null;
        }
        try {
            return enumAccessor.getStringValue(enumValue);
        } catch (IllegalArgumentException ex) {
            // Fallback to .name()
            return enumValue.name();
        }
    }
}
