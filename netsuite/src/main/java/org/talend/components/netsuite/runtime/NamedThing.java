package org.talend.components.netsuite.runtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
@EqualsAndHashCode
public class NamedThing {

    protected String name;

    protected transient String displayName;
}
