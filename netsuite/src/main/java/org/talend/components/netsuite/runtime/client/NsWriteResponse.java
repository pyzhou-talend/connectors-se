package org.talend.components.netsuite.runtime.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NsWriteResponse<RefT> {

    private NsStatus status;

    private RefT ref;
}
