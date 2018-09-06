package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.*;

@Slf4j
@Documentation("Schema discovering class")
public class MagentoCmsSchemaDiscover implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    public MagentoCmsSchemaDiscover(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
    }

    public List<String> getColumns() {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = configuration.getMagentoUrl();

        try {
            Iterator<JsonObject> dataArrayIterator = magentoHttpServiceFactory
                    .createMagentoHttpService(magentoUrl, configuration.getMagentoCmsConfigurationBase())
                    .getRecords(allParameters).iterator();
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