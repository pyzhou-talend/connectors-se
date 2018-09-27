package org.talend.components.onedrive.sources.list;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.service.configuration.ConfigurationService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Documentation("Schema discovering class")
@Service
public class SchemaDiscoverList implements Serializable {

    @Service
    private ConfigurationService configuration;

    @Service
    private OneDriveHttpClientService oneDriveHttpClientService;

    public List<String> getColumns() {
        List<String> result = new ArrayList<>();

        // // filter parameters
        // Map<String, String> allParameters = new TreeMap<>();
        // allParameters.put("searchCriteria[pageSize]", "1");
        // allParameters.put("searchCriteria[currentPage]", "1");
        //
        // String magentoUrl = configuration.getConfiguration().getMagentoUrl();
        //
        // try {
        // Iterator<JsonObject> dataArrayIterator = oneDriveHttpClientService.getRecords(magentoUrl, allParameters).iterator();
        // if (dataArrayIterator.hasNext()) {
        // JsonValue val = dataArrayIterator.next();
        // val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
        // }
        // } catch (Exception e) {
        // log.error(e.getMessage());
        // }
        return result;
    }
}