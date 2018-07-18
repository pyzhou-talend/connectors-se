package org.talend.components.netsuite.runtime.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Holds information required for logging in of a client in NetSuite.
 */
@Data
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class NetSuiteCredentials {

    private String email;

    private String password;

    private String account;

    private String roleId;

    private String applicationId;

    private int numberOfSeats = 1;

    private String id;

    private String companyId;

    private String userId;

    private String partnerId;

    private String privateKey; // path to private key in der format

    private boolean useSsoLogin = false;

    public NetSuiteCredentials(String email, String password, String account, String roleId) {
        this(email, password, account, roleId, 1);
    }

    public NetSuiteCredentials(String email, String password, String account, String roleId, int numberOfSeats) {
        this.email = email;
        this.password = password;
        this.account = account;
        this.roleId = roleId;
        this.numberOfSeats = numberOfSeats;
    }

    public static NetSuiteCredentials loadFromLocation(URI location, String propertyPrefix) throws IOException {
        InputStream stream;
        if (location.getScheme().equals("classpath")) {
            stream = NetSuiteCredentials.class.getResourceAsStream(location.getSchemeSpecificPart());
        } else {
            stream = location.toURL().openStream();
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } finally {
            stream.close();
        }
        return loadFromProperties(properties, propertyPrefix);
    }

    /**
     * Load credentials from plain {@code Properties}.
     *
     * @param properties properties object
     * @param prefix prefix for property keys, can be empty
     * @return credentials object
     */
    public static NetSuiteCredentials loadFromProperties(Properties properties, String prefix) {
        NetSuiteCredentials credentials = new NetSuiteCredentials();
        credentials.setEmail(properties.getProperty(prefix + "email"));
        credentials.setPassword(properties.getProperty(prefix + "password"));
        credentials.setRoleId(properties.getProperty(prefix + "roleId"));
        credentials.setAccount(properties.getProperty(prefix + "account"));
        credentials.setApplicationId(properties.getProperty(prefix + "applicationId"));
        return credentials;
    }

}