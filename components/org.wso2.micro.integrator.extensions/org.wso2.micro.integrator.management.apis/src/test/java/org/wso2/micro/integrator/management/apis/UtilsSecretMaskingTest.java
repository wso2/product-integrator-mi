/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.management.apis;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.config.xml.inbound.InboundEndpointSerializer;
import org.apache.synapse.inbound.InboundEndpoint;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import static org.wso2.micro.integrator.management.apis.Constants.PASSWORD_MASKED_VALUE;

/**
 * Tests the masking of the secrets which the management API exposes as a part of the synapse artifacts.
 */
public class UtilsSecretMaskingTest {

    private static final String SECRET = "ssMySecretPassword";

    @Test
    public void testSecretParameterNames() {

        Assert.assertTrue("A parameter named password should be masked",
                Utils.isSecretParameter("password"));
        Assert.assertTrue("A connector password parameter should be masked",
                Utils.isSecretParameter("solace.password"));
        Assert.assertTrue("A camel cased password parameter should be masked",
                Utils.isSecretParameter("sslPassword"));
        Assert.assertTrue("An upper cased password parameter should be masked",
                Utils.isSecretParameter("STORE_PASSWORD"));
        Assert.assertTrue("An OAuth client secret parameter should be masked",
                Utils.isSecretParameter("clientSecret"));
        Assert.assertTrue("A JNDI credentials parameter should be masked",
                Utils.isSecretParameter("java.naming.security.credentials"));
        Assert.assertTrue("A JMS transport credential parameter should be masked",
                Utils.isSecretParameter("naming_security_credential"));
    }

    @Test
    public void testNonSecretParameterNames() {

        Assert.assertFalse("A username parameter should not be masked",
                Utils.isSecretParameter("solace.username"));
        Assert.assertFalse("A port parameter should not be masked",
                Utils.isSecretParameter("inbound.http.port"));
        Assert.assertFalse("A null parameter name should not be masked",
                Utils.isSecretParameter(null));
        Assert.assertFalse("A token endpoint parameter should not be masked",
                Utils.isSecretParameter("oauth.token.endpoint"));
        Assert.assertFalse("A key store location parameter should not be masked",
                Utils.isSecretParameter("ws.ssl.keystore.location"));
        Assert.assertFalse("The allow credentials flag of a cors handler should not be masked",
                Utils.isSecretParameter("allowCredentials"));
        Assert.assertTrue("A parameter whose name merely contains a name which holds no secret should be masked",
                Utils.isSecretParameter("smtp.allowCredentials.password"));
    }

    @Test
    public void testSecretParameterNamesOfAConnectorConnection() {

        for (String name : new String[]{"securityToken", "accessToken", "refresh_token", "gspread_refresh_token",
                "passphrase", "apiKey", "api.key", "api_key", "privateKey", "private.key", "signature"}) {
            Assert.assertTrue("A connection parameter named " + name + " should be masked",
                    Utils.isSecretParameter(name));
        }
    }

    @Test
    public void testParameterNamesWhichOnlyDescribeASecretAreNotMasked() {

        for (String name : new String[]{"tokenUrl", "token.url", "tokenEndpoint", "oauth.token.endpoint", "tokenType",
                "signatureLength", "signatureAlgorithm", "passwordCallbackClass", "passwordType", "secretAlias",
                "privateKeyAlias"}) {
            Assert.assertFalse("A parameter named " + name + " describes a secret but holds none",
                    Utils.isSecretParameter(name));
        }
    }

    @Test
    public void testMaskSecretsOfAConnectorConnectionLocalEntry() throws XMLStreamException {

        OMElement connection = AXIOMUtil.stringToOM(
                "<salesforce.init xmlns=\"http://ws.apache.org/ns/synapse\">"
                        + "<username>connUser</username>"
                        + "<password>" + SECRET + "</password>"
                        + "<securityToken>" + SECRET + "</securityToken>"
                        + "<accessToken>" + SECRET + "</accessToken>"
                        + "<apiKey>" + SECRET + "</apiKey>"
                        + "<passphrase>" + SECRET + "</passphrase>"
                        + "<clientSecret>" + SECRET + "</clientSecret>"
                        + "<tokenUrl>https://login.example.com/services/oauth2/token</tokenUrl>"
                        + "</salesforce.init>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(connection).toString();

        Assert.assertFalse("No credential of the connection should be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The user of the connection should not be masked",
                maskedConfiguration.contains("connUser"));
        Assert.assertTrue("The token url of the connection should not be masked",
                maskedConfiguration.contains("https://login.example.com/services/oauth2/token"));
        Assert.assertTrue("The connection owned by the synapse configuration should not be modified",
                connection.toString().contains(SECRET));
    }

    @Test
    public void testMaskSecretAndCredentialParameters() {

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("java.naming.security.principal", "admin");
        parameters.put("java.naming.security.credentials", SECRET);
        parameters.put("clientSecret", SECRET);

        Map<String, Object> maskedParameters = Utils.maskPasswordParameters(parameters);

        Assert.assertEquals("The JNDI credentials should be masked",
                PASSWORD_MASKED_VALUE, maskedParameters.get("java.naming.security.credentials"));
        Assert.assertEquals("The client secret should be masked",
                PASSWORD_MASKED_VALUE, maskedParameters.get("clientSecret"));
        Assert.assertEquals("The principal should not be masked",
                "admin", maskedParameters.get("java.naming.security.principal"));
    }

    @Test
    public void testMaskPasswordParametersDoesNotModifyTheGivenMap() {

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("solace.password", SECRET);

        Utils.maskPasswordParameters(parameters);

        Assert.assertEquals("The parameter map used at runtime should not be modified",
                SECRET, parameters.get("solace.password"));
    }

    @Test
    public void testMaskCredentialsCarriedByAParameterMapValue() {

        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("transport.vfs.FileURI", "vfs:sftp://sftpuser:" + SECRET + "@localhost:22/in");
        parameters.put("store.jdbc.connection.url", "jdbc:postgresql://dbuser:" + SECRET + "@localhost:5432/mi");

        Map<String, Object> maskedParameters = Utils.maskPasswordParameters(parameters);

        String maskedFileUri = (String) maskedParameters.get("transport.vfs.FileURI");
        Assert.assertFalse("The credentials of the file uri should not be returned",
                maskedFileUri.contains(SECRET));
        Assert.assertTrue("The user of the file uri should not be masked",
                maskedFileUri.contains("sftpuser"));
        Assert.assertTrue("The host of the file uri should not be masked",
                maskedFileUri.contains("localhost:22/in"));

        String maskedConnectionUrl = (String) maskedParameters.get("store.jdbc.connection.url");
        Assert.assertFalse("The credentials of the connection url should not be returned",
                maskedConnectionUrl.contains(SECRET));
        Assert.assertTrue("The user of the connection url should not be masked",
                maskedConnectionUrl.contains("dbuser"));
    }

    @Test
    public void testMaskSecretValueLeavesValuesWhichCarryNoSecret() {

        Assert.assertEquals("The value of a secret parameter should be masked",
                PASSWORD_MASKED_VALUE, Utils.maskSecretValue("solace.password", SECRET));
        Assert.assertEquals("A value which carries no secret should be returned as it is",
                "http://localhost:8290/backend",
                Utils.maskSecretValue("endpoint.uri", "http://localhost:8290/backend"));
        Assert.assertEquals("A value which is not a string should be returned as it is",
                Integer.valueOf(8290), Utils.maskSecretValue("transport.vfs.Port", Integer.valueOf(8290)));
        Assert.assertNull("A null value of a parameter which is not a secret should be returned as it is",
                Utils.maskSecretValue("transport.vfs.FileURI", null));

        String vaultUri = "vfs:sftp://{wso2:vault-decrypt('encryptedValue')}@localhost:22/in";
        Assert.assertEquals("A uri which resolves its secret at runtime should be left as it is",
                vaultUri, Utils.maskSecretValue("transport.vfs.FileURI", vaultUri));
    }

    @Test
    public void testMaskPasswordSerializedAsAChildElement() throws XMLStreamException {

        OMElement inboundEndpoint = AXIOMUtil.stringToOM(
                "<inboundEndpoint xmlns=\"http://ws.apache.org/ns/synapse\" name=\"OrderQueueListener\">"
                        + "<parameters>"
                        + "<parameter name=\"solace.password\"><secret>" + SECRET + "</secret></parameter>"
                        + "</parameters>"
                        + "</inboundEndpoint>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(inboundEndpoint).toString();

        Assert.assertFalse("A password serialized as a child element should be removed",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The password should be replaced with the masked value",
                maskedConfiguration.contains(PASSWORD_MASKED_VALUE));
    }

    @Test
    public void testParameterResolvedViaRegistryKeyIsNotModified() throws XMLStreamException {

        OMElement inboundEndpoint = AXIOMUtil.stringToOM(
                "<inboundEndpoint xmlns=\"http://ws.apache.org/ns/synapse\" name=\"OrderQueueListener\">"
                        + "<parameters>"
                        + "<parameter name=\"solace.password\" key=\"conf:/repository/password\"/>"
                        + "</parameters>"
                        + "</inboundEndpoint>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(inboundEndpoint).toString();

        Assert.assertTrue("The registry key should be preserved",
                maskedConfiguration.contains("conf:/repository/password"));
        Assert.assertFalse("A parameter which has no serialized value should not be masked",
                maskedConfiguration.contains(PASSWORD_MASKED_VALUE));
    }

    /**
     * Masks a configuration produced by the actual synapse serializer instead of a hand written one, so that the
     * masking is verified against the element structure which is really returned via the management API.
     */
    @Test
    public void testMaskPasswordsInASerializedInboundEndpoint() {

        InboundEndpoint inboundEndpoint = new InboundEndpoint();
        inboundEndpoint.setName("OrderQueueListener");
        inboundEndpoint.setProtocol("http");
        inboundEndpoint.addParameter("solace.username", "default");
        inboundEndpoint.addParameter("solace.password", SECRET);

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(
                InboundEndpointSerializer.serializeInboundEndpoint(inboundEndpoint)).toString();

        Assert.assertFalse("The serialized configuration should not contain the password",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The password should be replaced with the masked value",
                maskedConfiguration.contains(PASSWORD_MASKED_VALUE));
        Assert.assertTrue("The configuration should still contain the non secret parameters",
                maskedConfiguration.contains("default"));
    }

    @Test
    public void testMaskSecretElementsOfAnEndpoint() throws XMLStreamException {

        OMElement endpoint = AXIOMUtil.stringToOM(
                "<endpoint xmlns=\"http://ws.apache.org/ns/synapse\" name=\"SecureHttpEndpoint\">"
                        + "<http method=\"GET\" uri-template=\"http://localhost:8290/backend\">"
                        + "<authentication>"
                        + "<basicAuth><username>backendUser</username><password>" + SECRET + "</password></basicAuth>"
                        + "<oauth><clientCredentials><clientId>myClientId</clientId>"
                        + "<clientSecret>" + SECRET + "</clientSecret></clientCredentials></oauth>"
                        + "</authentication></http></endpoint>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(endpoint).toString();

        Assert.assertFalse("The basic auth password and the client secret should not be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The username should not be masked", maskedConfiguration.contains("backendUser"));
        Assert.assertTrue("The client id should not be masked", maskedConfiguration.contains("myClientId"));
    }

    @Test
    public void testMaskDatabasePoolPasswordOfAMediator() throws XMLStreamException {

        OMElement sequence = AXIOMUtil.stringToOM(
                "<sequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"DbSecretSequence\"><dblookup>"
                        + "<connection><pool>"
                        + "<driver>com.mysql.jdbc.Driver</driver>"
                        + "<url>jdbc:mysql://localhost:3306/testdb</url>"
                        + "<user>dbuser</user>"
                        + "<password>" + SECRET + "</password>"
                        + "</pool></connection></dblookup></sequence>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(sequence).toString();

        Assert.assertFalse("The database password should not be returned", maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The database user should not be masked", maskedConfiguration.contains("dbuser"));
        Assert.assertTrue("The connection url should not be masked",
                maskedConfiguration.contains("jdbc:mysql://localhost:3306/testdb"));
    }

    @Test
    public void testMaskSecretHeldByAValueAttribute() throws XMLStreamException {

        OMElement sequence = AXIOMUtil.stringToOM(
                "<sequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"PropertySequence\">"
                        + "<property name=\"my.password\" scope=\"default\" value=\"" + SECRET + "\"/>"
                        + "<property name=\"my.status\" scope=\"default\" value=\"active\"/>"
                        + "</sequence>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(sequence).toString();

        Assert.assertFalse("A secret held by a value attribute should not be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The secret value attribute should be masked",
                maskedConfiguration.contains(PASSWORD_MASKED_VALUE));
        Assert.assertTrue("A property which is not a secret should not be masked",
                maskedConfiguration.contains("active"));
    }

    @Test
    public void testMaskCredentialsCarriedByAUriAttribute() throws XMLStreamException {

        OMElement endpoint = AXIOMUtil.stringToOM(
                "<endpoint xmlns=\"http://ws.apache.org/ns/synapse\" name=\"UriCredentialsEndpoint\">"
                        + "<address uri=\"https://uriuser:" + SECRET + "@backend.example.com/service\"/>"
                        + "</endpoint>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(endpoint).toString();

        Assert.assertFalse("The credentials of the uri should not be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The user of the uri should not be masked", maskedConfiguration.contains("uriuser"));
        Assert.assertTrue("The host of the uri should not be masked",
                maskedConfiguration.contains("backend.example.com"));
    }

    @Test
    public void testMaskCredentialsCarriedByAParameterValue() throws XMLStreamException {

        OMElement proxy = AXIOMUtil.stringToOM(
                "<proxy xmlns=\"http://ws.apache.org/ns/synapse\" name=\"VfsSecretProxy\">"
                        + "<parameter name=\"transport.vfs.FileURI\">"
                        + "vfs:sftp://sftpuser:" + SECRET + "@localhost:22/in</parameter>"
                        + "</proxy>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(proxy).toString();

        Assert.assertFalse("The credentials of the file uri should not be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The user of the file uri should not be masked",
                maskedConfiguration.contains("sftpuser"));
    }

    @Test
    public void testMaskCredentialsOfAUriWhoseSecretCarriesAnAtSign() throws XMLStreamException {

        String secretWithAtSign = "ssMy@SecretPasswordTail";
        OMElement proxy = AXIOMUtil.stringToOM(
                "<proxy xmlns=\"http://ws.apache.org/ns/synapse\" name=\"VfsAtSignProxy\">"
                        + "<parameter name=\"transport.vfs.FileURI\">"
                        + "vfs:sftp://sftpuser:" + secretWithAtSign + "@localhost:22/in</parameter>"
                        + "</proxy>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(proxy).toString();

        Assert.assertFalse("The credentials of the file uri should not be returned",
                maskedConfiguration.contains(secretWithAtSign));
        Assert.assertFalse("The part of the secret which follows the at sign should not be returned",
                maskedConfiguration.contains("SecretPasswordTail"));
        Assert.assertTrue("The user of the file uri should not be masked",
                maskedConfiguration.contains("sftpuser"));
        Assert.assertTrue("The host of the file uri should not be masked",
                maskedConfiguration.contains("localhost:22/in"));
    }

    @Test
    public void testCorsAllowCredentialsFlagIsNotMasked() throws XMLStreamException {

        OMElement api = AXIOMUtil.stringToOM(
                "<api xmlns=\"http://ws.apache.org/ns/synapse\" name=\"CorsApi\"><handlers>"
                        + "<handler class=\"org.wso2.micro.integrator.security.handler.CORSRequestHandler\">"
                        + "<property name=\"allowCredentials\" value=\"true\"/>"
                        + "</handler></handlers></api>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(api).toString();

        Assert.assertTrue("The allow credentials flag of a cors handler should not be masked",
                maskedConfiguration.contains("value=\"true\""));
    }

    @Test
    public void testMaskPasswordsInADataServiceConfiguration() throws XMLStreamException {

        OMElement dataService = AXIOMUtil.stringToOM(
                "<data name=\"SampleDataService\"><config id=\"default\">"
                        + "<property name=\"org.wso2.ws.dataservice.driver\">org.h2.Driver</property>"
                        + "<property name=\"org.wso2.ws.dataservice.user\">wso2ds</property>"
                        + "<property name=\"org.wso2.ws.dataservice.password\">" + SECRET + "</property>"
                        + "</config></data>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(dataService).toString();

        Assert.assertFalse("The password of the data service config should not be returned",
                maskedConfiguration.contains(SECRET));
        Assert.assertTrue("The driver of the data service config should not be masked",
                maskedConfiguration.contains("org.h2.Driver"));
        Assert.assertTrue("The user of the data service config should not be masked",
                maskedConfiguration.contains("wso2ds"));
    }

    @Test
    public void testUriResolvedThroughSecureVaultIsNotMasked() throws XMLStreamException {

        String vaultUri = "vfs:sftp://{wso2:vault-decrypt('encryptedValue')}@localhost:22/in";
        OMElement proxy = AXIOMUtil.stringToOM(
                "<proxy xmlns=\"http://ws.apache.org/ns/synapse\" name=\"VaultProxy\">"
                        + "<parameter name=\"transport.vfs.FileURI\">" + vaultUri + "</parameter></proxy>");

        String maskedConfiguration = Utils.maskPasswordsInConfiguration(proxy).toString();

        Assert.assertTrue("A uri which resolves its secret at runtime should be left as it is",
                maskedConfiguration.contains(vaultUri));
    }

    @Test
    public void testMaskPasswordsInConfigurationDoesNotModifyTheGivenElement() throws XMLStreamException {

        OMElement proxy = AXIOMUtil.stringToOM(
                "<proxy xmlns=\"http://ws.apache.org/ns/synapse\" name=\"VfsSecretProxy\">"
                        + "<parameter name=\"custom.password\">" + SECRET + "</parameter></proxy>");

        Utils.maskPasswordsInConfiguration(proxy);

        Assert.assertTrue("The configuration owned by the synapse configuration should not be modified",
                proxy.toString().contains(SECRET));
    }

    @Test
    public void testMaskSecretsInJsonRepresentation() {

        JSONObject endpointJson = new JSONObject();
        endpointJson.put("name", "UriCredentialsEndpoint");
        endpointJson.put("address", "https://uriuser:" + SECRET + "@backend.example.com/service");
        endpointJson.put("clientSecret", SECRET);
        JSONObject advanced = new JSONObject();
        advanced.put("password", SECRET);
        endpointJson.put("advanced", advanced);
        JSONArray children = new JSONArray();
        JSONObject child = new JSONObject();
        child.put("address", "https://childuser:" + SECRET + "@child.example.com/service");
        children.put(child);
        endpointJson.put("children", children);

        JSONObject maskedJson = Utils.maskSecretsInJson(endpointJson);

        Assert.assertFalse("No secret should be returned in the json representation",
                maskedJson.toString().contains(SECRET));
        Assert.assertEquals("A field which denotes a secret should be masked",
                PASSWORD_MASKED_VALUE, maskedJson.getString("clientSecret"));
        Assert.assertEquals("A secret nested in the json representation should be masked",
                PASSWORD_MASKED_VALUE, maskedJson.getJSONObject("advanced").getString("password"));
        Assert.assertTrue("The user of the address should not be masked",
                maskedJson.getString("address").contains("uriuser"));
        Assert.assertFalse("A secret nested in a json array should be masked",
                maskedJson.getJSONArray("children").getJSONObject(0).getString("address").contains(SECRET));
        Assert.assertTrue("The json representation cached by the artifact should not be modified",
                endpointJson.toString().contains(SECRET));
    }

    @Test
    public void testNullConfigurationIsHandled() {

        Assert.assertNull("A null configuration should be returned as it is",
                Utils.maskPasswordsInConfiguration(null));
    }

    @Test
    public void testSecretsOfTheInboundEndpointParameterListAreMasked() throws Exception {

        InboundEndpoint inboundEndpoint = new InboundEndpoint();
        inboundEndpoint.setName("VfsInbound");
        inboundEndpoint.setProtocol("file");
        inboundEndpoint.setInjectingSeq("main");
        inboundEndpoint.configure(new AspectConfiguration("VfsInbound"));
        inboundEndpoint.addParameter("transport.vfs.FileURI", "vfs:sftp://sftpuser:" + SECRET + "@localhost:22/in");
        inboundEndpoint.addParameter("transport.vfs.Password", SECRET);
        inboundEndpoint.addParameter("transport.vfs.ActionAfterProcess", "MOVE");

        JSONArray parameters = convertInboundEndpointToJson(inboundEndpoint).getJSONArray("parameters");

        Assert.assertEquals("The value of a secret parameter should be masked",
                PASSWORD_MASKED_VALUE, parameterValue(parameters, "transport.vfs.Password"));

        String maskedFileUri = (String) parameterValue(parameters, "transport.vfs.FileURI");
        Assert.assertFalse("The credentials of the file uri should not be returned",
                maskedFileUri.contains(SECRET));
        Assert.assertTrue("The user of the file uri should not be masked",
                maskedFileUri.contains("sftpuser"));
        Assert.assertTrue("The host of the file uri should not be masked",
                maskedFileUri.contains("localhost:22/in"));

        Assert.assertEquals("A parameter which holds no secret should not be masked",
                "MOVE", parameterValue(parameters, "transport.vfs.ActionAfterProcess"));
    }

    /**
     * The inbound endpoint resource builds its parameter list itself rather than through the masking helpers, hence
     * it is covered here. The conversion is an internal detail of the resource, therefore it is invoked reflectively
     * rather than widening its visibility only for this test.
     */
    private JSONObject convertInboundEndpointToJson(InboundEndpoint inboundEndpoint) throws Exception {

        Method conversion = InboundEndpointResource.class
                .getDeclaredMethod("convertInboundEndpointToJsonObject", InboundEndpoint.class);
        conversion.setAccessible(true);
        return (JSONObject) conversion.invoke(new InboundEndpointResource("/inbound-endpoints"), inboundEndpoint);
    }

    private Object parameterValue(JSONArray parameters, String parameterName) {

        for (int i = 0; i < parameters.length(); i++) {
            JSONObject parameter = parameters.getJSONObject(i);
            if (parameterName.equals(parameter.getString(Constants.NAME))) {
                return parameter.get("value");
            }
        }
        Assert.fail("The parameter " + parameterName + " is not returned");
        return null;
    }
}
