package org.talend.components.netsuite.runtime.model.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SearchFieldOperatorName {

    private String dataType;

    private String name;

    public SearchFieldOperatorName(String qualifiedName) {
        int i = qualifiedName.indexOf('.');
        if (i == -1) {
            this.dataType = qualifiedName;
            this.name = null;
        } else {
            String tempDataType = qualifiedName.substring(0, i);
            if (tempDataType.isEmpty()) {
                throw new IllegalArgumentException("Invalid operator data type: " + "'" + tempDataType + "'");
            }
            String tempName = qualifiedName.substring(i + 1);
            if (tempName.isEmpty()) {
                throw new IllegalArgumentException("Invalid operator name: " + "'" + tempName + "'");
            }

            this.dataType = tempDataType;
            this.name = tempName;
        }
    }

    public String getQualifiedName() {
        return name != null ? dataType + '.' + name : dataType;
    }
}
