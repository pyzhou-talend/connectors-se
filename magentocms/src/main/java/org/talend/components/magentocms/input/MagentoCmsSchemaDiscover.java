package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Documentation("Schema discovering class")
@Service
public class MagentoCmsSchemaDiscover implements Serializable {

    @Service
    private MagentoHttpClientService magentoHttpClientService = null;

    public List<String> getColumns(MagentoCmsInputMapperConfiguration configuration) {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = configuration.getMagentoUrl();

        try {
            Iterator<JsonObject> dataArrayIterator = magentoHttpClientService
                    .getRecords(configuration.getMagentoDataStore(), magentoUrl, allParameters).iterator();
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