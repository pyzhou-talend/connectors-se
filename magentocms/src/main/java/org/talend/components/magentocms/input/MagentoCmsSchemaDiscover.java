package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.service.ConfigurationServiceInput;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.*;

@Slf4j
@Documentation("Schema discovering class")
@Service
public class MagentoCmsSchemaDiscover implements Serializable {

    @Service
    private ConfigurationServiceInput configuration;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    public List<String> getColumns() {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = configuration.getMagentoCmsInputMapperConfiguration().getMagentoUrl();

        try {
            Iterator<JsonObject> dataArrayIterator = magentoHttpClientService.getRecords(magentoUrl, allParameters).iterator();
            if (dataArrayIterator.hasNext()) {
                JsonValue val = dataArrayIterator.next();
                val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return result;
    }
}