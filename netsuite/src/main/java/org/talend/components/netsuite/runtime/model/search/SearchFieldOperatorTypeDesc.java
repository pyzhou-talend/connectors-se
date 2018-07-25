package org.talend.components.netsuite.runtime.model.search;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.talend.components.netsuite.runtime.model.beans.Beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SearchFieldOperatorTypeDesc<T> {

    private SearchFieldOperatorType operatorType;

    private Class<T> operatorClass;

    private Function<T, String> mapper;

    private Function<String, T> reverseMapper;

    public SearchFieldOperatorName getOperatorName(T value) {
        return operatorType == SearchFieldOperatorType.BOOLEAN
                ? new SearchFieldOperatorName(SearchBooleanFieldOperator.BOOLEAN.getValue())
                : new SearchFieldOperatorName(operatorType.getDataType(), mapper.apply(value));
    }

    public List<SearchFieldOperatorName> getOperatorNames() {
        if (operatorClass.isEnum()) {
            return Arrays.stream(operatorClass.getEnumConstants()).map(this::getOperatorName).collect(Collectors.toList());
        } else {
            throw new IllegalStateException("Unsupported operator type: " + operatorClass);
        }
    }

    public T getOperator(String qualifiedName) {
        SearchFieldOperatorName opName = new SearchFieldOperatorName(qualifiedName);
        if (operatorType == SearchFieldOperatorType.BOOLEAN) {
            if (SearchFieldOperatorType.BOOLEAN.getOperatorTypeName().equals(opName)) {
                return (T) SearchBooleanFieldOperator.BOOLEAN;
            }
            throw new IllegalArgumentException(
                    "Invalid operator type: " + "'" + qualifiedName + "' != '" + opName.getDataType() + "'");
        } else {
            if (operatorType.dataTypeEquals(opName.getDataType())) {
                return reverseMapper.apply(opName.getName());
            }
            throw new IllegalArgumentException(
                    "Invalid operator data type: " + "'" + opName.getDataType() + "' != '" + operatorType.getDataType() + "'");
        }
    }

    public boolean hasOperator(SearchFieldOperatorName operatorName) {
        return operatorType.getDataType().equals(operatorName.getDataType());
    }

    public List<T> getOperators() {
        if (operatorClass.isEnum()) {
            return Arrays.asList(operatorClass.getEnumConstants());
        } else {
            throw new IllegalStateException("Unsupported operator type: " + operatorClass);
        }
    }

    /**
     * Create search field operator descriptor for NetSuite's search field operator enum class.
     *
     * @param operatorType type of operator
     * @param clazz enum class
     * @param <T> type of search operator data object
     * @return search field operator descriptor
     */
    public static <T> SearchFieldOperatorTypeDesc<T> createForEnum(SearchFieldOperatorType operatorType, Class<T> clazz) {
        return new SearchFieldOperatorTypeDesc<>(operatorType, clazz,
                (Function<T, String>) Beans.getEnumToStringMapper((Class<Enum>) clazz),
                (Function<String, T>) Beans.getEnumFromStringMapper((Class<Enum>) clazz));
    }
}