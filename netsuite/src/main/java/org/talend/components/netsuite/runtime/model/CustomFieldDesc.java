package org.talend.components.netsuite.runtime.model;

import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Descriptor of custom field which is not declared as {@code member field} in data object type class
 * and stored in {@code customFieldList}.
 */

@Data
@EqualsAndHashCode(callSuper = false)
@ToString
public class CustomFieldDesc extends FieldDesc {

    /** Customization ref for this field. */
    private NsRef customizationRef;

    /** Type of this custom field. */
    private CustomFieldRefType customFieldType;
}
