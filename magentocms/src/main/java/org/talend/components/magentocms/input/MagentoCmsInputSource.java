package org.talend.components.magentocms.input;

import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Documentation("Input data processing class")
public class MagentoCmsInputSource implements Serializable {

    private final MagentoInputConfiguration configuration;

    private final MagentoHttpClientService magentoHttpClientService;

    private InputIterator inputIterator;

    public MagentoCmsInputSource(@Option("configuration") final MagentoInputConfiguration configuration,
            final MagentoHttpClientService magentoHttpClientService) {
        this.configuration = configuration;
        this.magentoHttpClientService = magentoHttpClientService;
    }

    @PostConstruct
    public void init() throws IOException {
        // parameters
        Map<String, String> allParameters = new TreeMap<>();
        boolean isAdvancedFilter = configuration.getSelectionFilter().getFilterAdvancedValueWrapper() != null
                && configuration.getSelectionFilter().getFilterAdvancedValueWrapper().getFilterAdvancedValue() != null
                && !configuration.getSelectionFilter().getFilterAdvancedValueWrapper().getFilterAdvancedValue().trim().isEmpty();
        if (!isAdvancedFilter) {
            ConfigurationHelper.fillFilterParameters(allParameters, configuration.getSelectionFilter(), true);
        }

        // String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
        // .collect(Collectors.joining("&"));
        if (isAdvancedFilter) {
            // allParametersStr += allParametersStr.isEmpty() ? "" : "&";
            // allParametersStr += encodeValue(configuration.getSelectionFilter().getFilterAdvancedValue().trim());
            String advancedFilterText = encodeValue(
                    configuration.getSelectionFilter().getFilterAdvancedValueWrapper().getFilterAdvancedValue().trim());
            Map<String, String> advancedFilterParameters = Arrays.stream(advancedFilterText.split("&"))
                    .map(item -> item.split("=")).collect(Collectors.toMap(item -> item[0], item -> item[1]));
            allParameters.putAll(advancedFilterParameters);
        }

        String magentoUrl = configuration.getMagentoUrl();
        // magentoUrl += "?" + allParametersStr;

        inputIterator = new InputIterator(magentoUrl, allParameters, magentoHttpClientService,
                configuration.getMagentoDataStore());
    }

    public String encodeValue(String filter) throws UnsupportedEncodingException {
        filter = filter.trim();
        StringBuffer filterEncoded = new StringBuffer();
        Pattern p = Pattern.compile("(\\[value\\]=)(.*?)(&|$)");
        Matcher m = p.matcher(filter);
        while (m.find()) {
            String rep = m.group(1) + URLEncoder.encode(m.group(2), "UTF-8") + m.group(3);
            m.appendReplacement(filterEncoded, rep);
        }
        m.appendTail(filterEncoded);
        return filterEncoded.toString();
    }

    @Producer
    public JsonObject next() {
        if (inputIterator != null && inputIterator.hasNext()) {
            JsonValue val = inputIterator.next();
            return val.asJsonObject();
        }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}