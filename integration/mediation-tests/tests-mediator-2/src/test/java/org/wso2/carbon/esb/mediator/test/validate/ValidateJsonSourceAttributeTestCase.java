/*
 *  Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.esb.mediator.test.validate;

import org.apache.http.HttpResponse;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.esb.integration.common.utils.ESBIntegrationTest;
import org.wso2.esb.integration.common.utils.clients.SimpleHttpClient;

import java.io.IOException;

/**
 * Tests that the json-source attribute on the validate mediator correctly validates JSON
 * extracted from a multipart/form-data request into a context property. The bug only
 * manifests with multipart requests — not with a plain JSON body.
 */
public class ValidateJsonSourceAttributeTestCase extends ESBIntegrationTest {

    private static final byte[] DUMMY_FILE_CONTENT = "dummy binary content".getBytes();
    private SimpleHttpClient httpClient;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init();
        httpClient = new SimpleHttpClient();
    }

    @Test(groups = "wso2.esb",
            description = "Validate JSON from multipart form field without json-source=true")
    public void testValidJsonPropertyWithoutJsonSource() throws IOException {
        String metadataJson = "{\"id\":\"123\",\"title\":\"My Document\"}";
        HttpResponse response = httpClient.doPostWithMultipartFormData(
                getApiInvocationURL("validateMediatorJsonSource") + "/withoutSourceType",
                "metadata", metadataJson, "data.bin", DUMMY_FILE_CONTENT);
        String responseBody = SimpleHttpClient.responseEntityBodyToString(response);
        Assert.assertTrue(responseBody.contains("error"),
                "Without json-source=true, the validate mediator should not validate JSON from a multipart form field. Response: " + responseBody);
    }

    @Test(groups = "wso2.esb",
            description = "Validate JSON from multipart form field using json-source=true with valid payload")
    public void testValidJsonPropertyPassesValidation() throws IOException {
        String metadataJson = "{\"id\":\"123\",\"title\":\"My Document\"}";
        HttpResponse response = httpClient.doPostWithMultipartFormData(
                getApiInvocationURL("validateMediatorJsonSource"),
                "metadata", metadataJson, "data.bin", DUMMY_FILE_CONTENT);
        String responseBody = SimpleHttpClient.responseEntityBodyToString(response);
        Assert.assertTrue(responseBody.contains("success"),
                "Valid metadata in multipart form should pass JSON schema validation. Response: " + responseBody);
    }

    @Test(groups = "wso2.esb",
            description = "Validate JSON from multipart form field using json-source=true with missing required field")
    public void testMissingRequiredFieldFailsValidation() throws IOException {
        String metadataJson = "{\"id\":\"123\"}";
        HttpResponse response = httpClient.doPostWithMultipartFormData(
                getApiInvocationURL("validateMediatorJsonSource"),
                "metadata", metadataJson, "data.bin", DUMMY_FILE_CONTENT);
        String responseBody = SimpleHttpClient.responseEntityBodyToString(response);
        Assert.assertTrue(responseBody.contains("fail"),
                "Metadata missing required 'title' field should fail JSON schema validation. Response: " + responseBody);
    }

    @Test(groups = "wso2.esb",
            description = "Validate JSON from multipart form field using json-source=true with wrong field type")
    public void testWrongFieldTypeFailsValidation() throws IOException {
        String metadataJson = "{\"id\":123,\"title\":\"My Document\"}";
        HttpResponse response = httpClient.doPostWithMultipartFormData(
                getApiInvocationURL("validateMediatorJsonSource"),
                "metadata", metadataJson, "data.bin", DUMMY_FILE_CONTENT);
        String responseBody = SimpleHttpClient.responseEntityBodyToString(response);
        Assert.assertTrue(responseBody.contains("fail"),
                "Metadata with non-string 'id' should fail JSON schema validation. Response: " + responseBody);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.cleanup();
    }
}
