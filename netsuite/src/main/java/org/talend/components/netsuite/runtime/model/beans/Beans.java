package org.talend.components.netsuite.runtime.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.beanutils.expression.DefaultResolver;
import org.apache.commons.beanutils.expression.Resolver;

import lombok.Data;

/**
 * Provides methods for working with {@code beans}.
 */
public abstract class Beans {

    private static final ConcurrentMap<Class<?>, BeanInfo> BEAN_INFO_CACHE = new ConcurrentHashMap<>();

    private static final Resolver PROPERTY_RESOLVER = new DefaultResolver();

    /**
     * Get property descriptor for given instance of a bean and it's property name.
     *
     * @param target target bean which to get property for
     * @param name name of property
     * @return property descriptor or {@code null} if specified property was not found
     */
    public static PropertyInfo getPropertyInfo(Object target, String name) {
        BeanInfo beanInfo = getBeanInfo(target.getClass());
        return beanInfo != null ? beanInfo.getProperty(name) : null;
    }

    /**
     * Get bean descriptor for given class.
     *
     * @param clazz class
     * @return bean descriptor
     */
    public static BeanInfo getBeanInfo(Class<?> clazz) {
        BeanInfo beanInfo = BEAN_INFO_CACHE.get(clazz);
        if (beanInfo == null) {
            BeanInfo newBeanInfo = loadBeanInfoForClass(clazz);
            if (BEAN_INFO_CACHE.putIfAbsent(clazz, newBeanInfo) == null) {
                beanInfo = newBeanInfo;
            } else {
                beanInfo = BEAN_INFO_CACHE.get(clazz);
            }
        }
        return beanInfo;
    }

    private static BeanInfo loadBeanInfoForClass(Class<?> clazz) {
        try {
            List<PropertyInfo> properties = BeanIntrospector.getInstance().getProperties(clazz.getName());
            return new BeanInfo(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set a value for given bean's property.
     *
     * @param target target bean
     * @param expr property path
     * @param value value to be set
     */
    public static void setProperty(Object target, String expr, Object value) {
        try {
            Object current = target;
            if (PROPERTY_RESOLVER.hasNested(expr)) {
                String currExpr = expr;
                while (PROPERTY_RESOLVER.hasNested(currExpr)) {
                    String next = PROPERTY_RESOLVER.next(currExpr);
                    Object obj = getSimpleProperty(current, next);
                    if (obj != null) {
                        current = obj;
                        currExpr = PROPERTY_RESOLVER.remove(currExpr);
                    }
                }
                if (current != null) {
                    setSimpleProperty(current, currExpr, value);
                }
            } else {
                if (current != null) {
                    setSimpleProperty(current, expr, value);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get value of given bean's property.
     *
     * @param target target bean
     * @param expr property path
     * @return value
     */
    public static Object getProperty(Object target, String expr) {
        try {
            Object current = target;
            if (PROPERTY_RESOLVER.hasNested(expr)) {
                String currExpr = expr;
                while (PROPERTY_RESOLVER.hasNested(currExpr) && current != null) {
                    String next = PROPERTY_RESOLVER.next(currExpr);
                    current = getSimpleProperty(current, next);
                    currExpr = PROPERTY_RESOLVER.remove(currExpr);
                }
                return current != null ? getSimpleProperty(current, currExpr) : null;
            } else {
                return getSimpleProperty(current, expr);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get value of given bean's property.
     *
     * @param target target bean
     * @param name name of property
     * @return value
     */
    public static Object getSimpleProperty(Object target, String name) {
        return getPropertyAccessor(target).get(target, name);
    }

    /**
     * Set a value for given bean's property.
     *
     * @param target target bean
     * @param name name of property
     * @param value to be set
     */
    public static void setSimpleProperty(Object target, String name, Object value) {
        getPropertyAccessor(target).set(target, name, value);
    }

    /**
     * Get property accessor for given object.
     *
     * @param target target object
     * @param <T> type of object
     * @return property accessor
     */
    @SuppressWarnings("unchecked")
    protected static <T> PropertyAccessor<T> getPropertyAccessor(T target) {
        if (target instanceof PropertyAccess) {
            return (PropertyAccessor<T>) ((PropertyAccess) target).getPropertyAccessor();
        } else {
            return (PropertyAccessor<T>) ReflectPropertyAccessor.INSTANCE;
        }
    }

    /**
     * Get enum accessor for given enum class.
     *
     * @param clazz enum class
     * @return enum accessor
     */
    public static EnumAccessor getEnumAccessor(Class<Enum<?>> clazz) {
        return getEnumAccessorImpl(clazz);
    }

    /**
     * Get enum accessor for given enum class.
     *
     * @param clazz enum class
     * @return enum accessor
     */
    protected static <T extends Enum<?>> AbstractEnumAccessor getEnumAccessorImpl(Class<Enum<?>> clazz) {
        EnumAccessor accessor = null;
        Method m;
        try {
            m = clazz.getDeclaredMethod("getEnumAccessor", new Class[0]);
            if (!m.getReturnType().equals(EnumAccessor.class)) {
                throw new NoSuchMethodException();
            }
            try {
                accessor = (EnumAccessor) m.invoke(new Object[0]);
            } catch (IllegalAccessException | InvocationTargetException e) {
                // TODO: Add logging
                // LOG.error("Failed to get optimized EnumAccessor: enum class " + clazz.getName(), e);
            }
        } catch (NoSuchMethodException e) {
        }
        if (accessor != null) {
            return new OptimizedEnumAccessor(clazz, accessor);
        } else {
            return new ReflectEnumAccessor(clazz);
        }
    }

    /**
     * Get mapper which maps an enum constant to string value.
     *
     * @param clazz enum class
     * @return mapper
     */
    public static Function<Enum<?>, String> getEnumToStringMapper(Class<Enum<?>> clazz) {
        return getEnumAccessorImpl(clazz).getToStringMapper();
    }

    /**
     * Get mapper which maps string value to an enum constant.
     *
     * @param clazz enum class
     * @return mapper
     */
    public static Function<String, Enum<?>> getEnumFromStringMapper(Class<Enum<?>> clazz) {
        return getEnumAccessorImpl(clazz).getFromStringMapper();
    }

    /**
     * Convert initial letter of given string value to upper case.
     *
     * @param value source value to be converted
     * @return converted value
     */
    public static String toInitialUpper(String value) {
        return value.substring(0, 1).toUpperCase() + value.substring(1);
    }

    /**
     * Convert initial letter of given string value to lower case.
     *
     * @param value source value to be converted
     * @return converted value
     */
    public static String toInitialLower(String value) {
        return value.substring(0, 1).toLowerCase() + value.substring(1);
    }

    /**
     * Property accessor which uses reflection to access properties.
     */
    protected static class ReflectPropertyAccessor implements PropertyAccessor<Object> {

        protected static final ReflectPropertyAccessor INSTANCE = new ReflectPropertyAccessor();

        /** An empty class array */
        private static final Class<?>[] EMPTY_CLASS_PARAMETERS = new Class[0];

        /** An empty object array */
        private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

        @Override
        public Object get(Object target, String name) {
            if (name == null) {
                throw new IllegalArgumentException("No name specified for bean class '" + target.getClass() + "'");
            }

            // Retrieve the property getter method for the specified property
            BeanInfo metaData = Beans.getBeanInfo(target.getClass());
            PropertyInfo descriptor = metaData.getProperty(name);
            if (descriptor == null) {
                throw new IllegalArgumentException("Unknown property '" + name + "' on class '" + target.getClass() + "'");
            }
            Method readMethod = getReadMethod(target.getClass(), descriptor);
            if (readMethod == null) {
                throw new IllegalArgumentException(
                        "Property '" + name + "' has no getter method in class '" + target.getClass() + "'");
            }

            // Call the property getter and return the value
            try {
                Object value = invokeMethod(readMethod, target, EMPTY_OBJECT_ARRAY);
                return (value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void set(Object target, String name, Object value) {
            if (name == null) {
                throw new IllegalArgumentException("No name specified for bean class '" + target.getClass() + "'");
            }

            // Retrieve the property setter method for the specified property
            BeanInfo metaData = Beans.getBeanInfo(target.getClass());
            PropertyInfo descriptor = metaData.getProperty(name);
            if (descriptor == null) {
                throw new IllegalArgumentException("Unknown property '" + name + "' on class '" + target.getClass() + "'");
            }
            Method writeMethod = getWriteMethod(target.getClass(), descriptor);
            if (writeMethod == null) {
                throw new IllegalArgumentException(
                        "Property '" + name + "' has no setter method in class '" + target.getClass() + "'");
            }

            // Call the property setter method
            Object[] values = new Object[1];
            values[0] = value;

            try {
                invokeMethod(writeMethod, target, values);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        /** This just catches and wraps IllegalArgumentException. */
        private Object invokeMethod(Method method, Object bean, Object[] values)
                throws IllegalAccessException, InvocationTargetException {

            try {
                return method.invoke(bean, values);
            } catch (IllegalArgumentException cause) {
                if (bean == null) {
                    throw new IllegalArgumentException(
                            "No bean specified " + "- this should have been checked before reaching this method");
                }
                String valueString = "";
                if (values != null) {
                    for (int i = 0; i < values.length; i++) {
                        if (i > 0) {
                            valueString += ", ";
                        }
                        valueString += (values[i]).getClass().getName();
                    }
                }
                String expectedString = "";
                Class<?>[] parTypes = method.getParameterTypes();
                if (parTypes != null) {
                    for (int i = 0; i < parTypes.length; i++) {
                        if (i > 0) {
                            expectedString += ", ";
                        }
                        expectedString += parTypes[i].getName();
                    }
                }
                IllegalArgumentException e = new IllegalArgumentException("Cannot invoke " + method.getDeclaringClass().getName()
                        + "." + method.getName() + " on bean class '" + bean.getClass() + "' - " + cause.getMessage()
                        + " - had objects of type \"" + valueString + "\" but expected signature \"" + expectedString + "\"");
                throw e;
            }
        }

        /**
         * <p>
         * Return an accessible property getter method for this property,
         * if there is one; otherwise return <code>null</code>.
         * </p>
         *
         * @param clazz The class of the read method will be invoked on
         * @param descriptor Property descriptor to return a getter for
         * @return The read method
         */
        Method getReadMethod(Class<?> clazz, PropertyInfo descriptor) {
            return (MethodUtils.getAccessibleMethod(clazz, descriptor.getReadMethodName(), EMPTY_CLASS_PARAMETERS));
        }

        /**
         * <p>
         * Return an accessible property setter method for this property,
         * if there is one; otherwise return <code>null</code>.
         * </p>
         *
         * @param clazz The class of the read method will be invoked on
         * @param descriptor Property descriptor to return a setter for
         * @return The write method
         */
        Method getWriteMethod(Class<?> clazz, PropertyInfo descriptor) {
            return (MethodUtils.getAccessibleMethod(clazz, descriptor.getWriteMethodName(),
                    new Class[] { descriptor.getWriteType() }));
        }
    }

    /**
     * Base class for enum accessors.
     */
    @Data
    protected static abstract class AbstractEnumAccessor implements EnumAccessor {

        protected Class<Enum<?>> enumClass;

        protected Function<Enum<?>, String> toStringMapper;

        protected Function<String, Enum<?>> fromStringMapper;

        protected AbstractEnumAccessor(Class<Enum<?>> enumClass) {
            this.enumClass = enumClass;
            toStringMapper = input -> getStringValue(input);
            fromStringMapper = input -> getEnumValue(input);
        }
    }

    /**
     * Enum accessor which uses reflection to access enum values.
     *
     * <p>
     * Enum classes generated from NetSuite's XML schemas have following methods
     * to access enum values:
     * <ul>
     * <li>{@code String value()) - get NetSuite specific string value of enum</li>
     *
    <li>{@code Enum fromValue(String)) - get enum constant for NetSuite specific string value</li>
     *
    </ul>
     */
    public static class ReflectEnumAccessor extends AbstractEnumAccessor {

        public ReflectEnumAccessor(Class<Enum<?>> enumClass) {
            super(enumClass);
        }

        @Override
        public String getStringValue(Enum<?> enumValue) {
            try {
                return (String) MethodUtils.invokeExactMethod(enumValue, "value", null);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getTargetException();
                }
                throw new RuntimeException(e.getTargetException());
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Enum<?> getEnumValue(String value) {
            try {
                return (Enum<?>) MethodUtils.invokeExactStaticMethod(enumClass, "fromValue", value);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getTargetException();
                }
                throw new RuntimeException(e.getTargetException());
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class OptimizedEnumAccessor extends AbstractEnumAccessor {

        private EnumAccessor accessor;

        public OptimizedEnumAccessor(Class<Enum<?>> enumClass, EnumAccessor accessor) {
            super(enumClass);
            this.accessor = accessor;
        }

        @Override
        public String getStringValue(Enum<?> enumValue) {
            return accessor.getStringValue(enumValue);
        }

        @Override
        public Enum<?> getEnumValue(String value) {
            return accessor.getEnumValue(value);
        }
    }
}
