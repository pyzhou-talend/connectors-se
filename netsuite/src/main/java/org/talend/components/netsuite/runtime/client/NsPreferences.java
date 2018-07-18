package org.talend.components.netsuite.runtime.client;

import lombok.Data;

/**
 * Holds information about NetSuite client's general preferences.
 *
 * <p>
 * This data object mirrors NetSuite's native {@code Preferences} data object.
 */

@Data
public class NsPreferences {

    private Boolean warningAsError;

    private Boolean disableMandatoryCustomFieldValidation;

    private Boolean disableSystemNotesForCustomFields;

    private Boolean ignoreReadOnlyFields;

    private Boolean runServerSuiteScriptAndTriggerWorkflows;

}
