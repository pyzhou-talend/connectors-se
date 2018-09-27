package org.talend.components.onedrive.sources;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Reject implements Serializable {

    private String errorMessage;

    private Record record;
}
