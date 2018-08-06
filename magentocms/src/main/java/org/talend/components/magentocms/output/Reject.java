package org.talend.components.magentocms.output;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.json.JsonObject;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Reject implements Serializable {

    private int code;

    private String errorMessage;

    private String errorDetail;

    private JsonObject record;
}
