package org.talend.components.netsuite.runtime.model.beans;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BeansIntrospector {

    private BeansIntrospector() {

    }

    private static final BeansIntrospector INSTANCE = new BeansIntrospector();

    public static BeansIntrospector getInstanse() {
        return INSTANCE;
    }

    public List<PropertyInfo> getProperties(String className) throws ClassNotFoundException {
        Class<?> clazz = Class.forName(className);
        return new ArrayList<>(getProperties(getMethods(clazz)));
    }

    public Set<PropertyInfo> getProperties(Set<Method> methods) {
        return null;
    }

    private Set<Method> getMethods(Class<?> clazz) {
        return Arrays.stream(clazz.getMethods())
                .filter(method -> (method.getModifiers() & 0x449) == 1)
                .collect(Collectors.toSet());
        }
}
