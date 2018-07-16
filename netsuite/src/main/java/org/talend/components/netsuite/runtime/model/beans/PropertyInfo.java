package org.talend.components.netsuite.runtime.model.beans;

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
}
