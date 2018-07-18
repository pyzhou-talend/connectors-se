package org.talend.components.netsuite.runtime.client.search;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Holds data for single condition of a search query.
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SearchCondition {

    /** Name of target field. */
    private String fieldName;

    /** Name of search operator to be applied. */
    private String operatorName;

    /** Search values to be used. */
    private List<String> values;

    public SearchCondition(String fieldName, String operatorName) {
        this(fieldName, operatorName, null);
    }
}
