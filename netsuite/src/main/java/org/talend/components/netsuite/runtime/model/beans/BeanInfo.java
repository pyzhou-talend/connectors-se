package org.talend.components.netsuite.runtime.model.beans;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BeanInfo {

    /**
     * Properties of bean
     */
    private List<PropertyInfo> properties;

    private Map<String, PropertyInfo> propertyMap;

    public BeanInfo(List<PropertyInfo> properties) {
        this.properties = properties;
        propertyMap = properties.stream().collect(Collectors.toMap(PropertyInfo::getName, (element) -> element));
    }

    public List<PropertyInfo> getProperties() {
        return Collections.unmodifiableList(properties);
    }

    public Map<String, PropertyInfo> getPropertyMap() {
        return Collections.unmodifiableMap(propertyMap);
    }

    public PropertyInfo getProperty(String name) {
        return propertyMap.get(name);
    }

    @Override
    public String toString() {
        return new StringBuilder("BeanInfo{").append("properties=").append(properties).append('}').toString();
    }
}
