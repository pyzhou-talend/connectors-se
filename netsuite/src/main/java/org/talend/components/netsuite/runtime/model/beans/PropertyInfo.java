package org.talend.components.netsuite.runtime.model.beans;

import java.lang.reflect.Method;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class PropertyInfo {

    private String name;

    private Class<?> readType;

    private Class<?> writeType;

    private String readMethodName;

    private String writeMethodName;

    public PropertyInfo(String name, Class<?> readType, Class<?> writeType, Method readMethod, Method writeMethod) {
        this(name, readType, writeType, readMethod != null ? readMethod.getName() : null,
                writeMethod != null ? writeMethod.getName() : null);
    }
}
