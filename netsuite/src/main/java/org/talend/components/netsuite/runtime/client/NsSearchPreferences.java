package org.talend.components.netsuite.runtime.client;

import lombok.Data;

/**
 * Holds information about general NetSuite client's search preferences.
 *
 * <p>
 * This data object mirrors NetSuite's native {@code SearchPreferences} data object.
 */
@Data
public class NsSearchPreferences {

    private Boolean bodyFieldsOnly;

    private Boolean returnSearchColumns;

    private Integer pageSize;

}
