package org.talend.components.magentocms.input;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
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
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Documentation("TODO fill the documentation for this input")
public class MagentoCmsInputSource implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    // private Iterator<JsonObject> dataArrayIterator;

    private InputIterator inputIterator;

    public MagentoCmsInputSource(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        // parameters
        Map<String, String> allParameters = new TreeMap<>();
        if (configuration.getSelectionFilter().getFilterAdvancedValue().trim().isEmpty()) {
            ConfigurationHelper.fillFilterParameters(allParameters, configuration.getSelectionFilter(), true);
        }
        // fillFieldsParameters(allParameters, configuration.getSelectedFields());
        // StringBuilder allParametersStr = new StringBuilder();

        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        if (!configuration.getSelectionFilter().getFilterAdvancedValue().trim().isEmpty()) {
            allParametersStr += allParametersStr.isEmpty() ? "" : "&";
            allParametersStr += encodeValue(configuration.getSelectionFilter().getFilterAdvancedValue().trim());
        }

        // String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
        // + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
        // + configuration.getSelectionType().name().toLowerCase();
        String magentoUrl = configuration.getMagentoUrl();
        // magentoUrl += "?" + URLEncoder.encode(allParametersStr, "UTF-8");
        magentoUrl += "?" + allParametersStr;

        inputIterator = new InputIterator(magentoUrl,
                magentoHttpServiceFactory.createMagentoHttpService(configuration.getMagentoCmsConfigurationBase()));

        // try {
        // dataArrayIterator = magentoHttpServiceFactory.createMagentoHttpService(configuration.getMagentoCmsConfigurationBase())
        // .getRecords(magentoUrl).iterator();
        // } catch (BadRequestException e) {
        // System.err.println(e.getMessage());
        // }
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

    // private void fillFieldsParameters(Map<String, String> allParameters, String fields) {
    // if (fields != null && !fields.isEmpty()) {
    // allParameters.put("fields", fields);
    // }
    // }

    @Producer
    public JsonObject next() {
        // if (dataArrayIterator != null && dataArrayIterator.hasNext()) {
        // JsonValue val = dataArrayIterator.next();
        // return val.asJsonObject();
        // }
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