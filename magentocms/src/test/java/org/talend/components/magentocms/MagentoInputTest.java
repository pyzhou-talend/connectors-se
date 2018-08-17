package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.talend.components.magentocms.common.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class MagentoInputTest {

    @Test
    public void testConnection() throws UnknownAuthenticationTypeException {
        AuthenticationOauth1Settings authenticationOauth1Settings = new AuthenticationOauth1Settings();
        AuthenticationTokenSettings authenticationTokenSettings = new AuthenticationTokenSettings();
        AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings = new AuthenticationLoginPasswordSettings();
        MagentoCmsConfigurationBase magentoCmsConfigurationBase;
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationOauth1Settings, magentoCmsConfigurationBase.getAuthSettings());
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.AUTHENTICATION_TOKEN,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationTokenSettings, magentoCmsConfigurationBase.getAuthSettings());
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.LOGIN_PASSWORD,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationLoginPasswordSettings, magentoCmsConfigurationBase.getAuthSettings());
    }

    // @ParameterizedTest
    // @ValueSource(authTypes = { AuthenticationType.OAUTH_1, AuthenticationType.AUTHENTICATION_TOKEN,
    // AuthenticationType.LOGIN_PASSWORD })
    // void testGetAuth(String currentValue) {
    // // do test
    // }
    //
    // @ParameterizedTest
    // @MethodSource("stringIntAndListProvider")
    // void testWithMultiArgMethodSource(String str, int num, List<String> list) {
    // assertEquals(3, str.length());
    // assertTrue(num >=1 && num <=2);
    // assertEquals(2, list.size());
    // }
    //
    // static Stream<Arguments> stringIntAndListProvider() {
    // return Stream.of(
    // Arguments.of("foo", 1, Arrays.asList("a", "b")),
    // Arguments.of("bar", 2, Arrays.asList("x", "y"))
    // );
    // }
}
