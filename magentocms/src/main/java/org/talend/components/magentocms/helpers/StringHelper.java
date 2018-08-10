package org.talend.components.magentocms.helpers;

import java.util.Map;

public class StringHelper {

    public static String httpParametersMapToString(Map<String, String> allParameters) {
        StringBuilder allParametersStr = new StringBuilder();
        boolean addSeparator = false;
        for (Map.Entry entry : allParameters.entrySet()) {
            if (addSeparator) {
                allParametersStr.append("&");
            } else {
                addSeparator = true;
            }
            allParametersStr.append(entry.getKey() + "=" + entry.getValue());
        }
        return allParametersStr.toString();
    }
}
