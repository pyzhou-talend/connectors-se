package org.talend.components.magentocms.output;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Reject implements Serializable {

    private int code;

    private String errorMessage;

    private String errorDetail;

    private Record record;
}
