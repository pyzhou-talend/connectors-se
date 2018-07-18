package org.talend.components.netsuite.runtime.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NsReadResponse<RecT> {

    private NsStatus status;

    private RecT record;

}
