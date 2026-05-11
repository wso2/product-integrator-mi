/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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
package org.wso2.carbon.esb.message.processor.test;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.test.utils.http.client.HttpRequestUtil;
import org.wso2.esb.integration.common.extensions.carbonserver.CarbonServerExtension;
import org.wso2.esb.integration.common.utils.CarbonLogReader;
import org.wso2.esb.integration.common.utils.ESBIntegrationTest;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * The Message processor is configured with 200,202 as non retry status codes.
 * If the back end returns 500 SC, then the MP keeps on retrying till the max
 * delivery attempt is reached. This test case is used to verify that behavior.
 */
public class ESBJAVA4279_MPRetryUponResponseSC_500_withNonRetryStatusCodes_200_and_202_TestCase
        extends ESBIntegrationTest {

    private static final String EXPECTED_MP_DEACTIVATION_MSG =
            "Successfully deactivated the message processor [Processor1]";
    private static final int RETRY_COUNT = 4;
    private static final String CLIENT_REQUEST = "client-request";
    private CarbonLogReader carbonLogReader;

    @BeforeClass(alwaysRun = true)
    public void deployService() throws Exception {

        CarbonServerExtension.startServer();
        super.init();
        carbonLogReader = new CarbonLogReader();
        carbonLogReader.start();
    }

    @Test(groups = { "wso2.esb" },
            description = "Test whether a Message Processor retries sending the message to the EP when the "
                    + "response status code is 500 and MP is configured with 200,202 as non-retry status codes.")
    public void testMPRetryUponHTTP_SC_500_response_with_200_And_202_AsNonRetrySCs() throws Exception {

        String errorMsg =
                "Message processor [Processor1] received a response with HTTP_SC: 500 from backend EP. Message forwarding failed.";
        clearLogsAndSendRequest("NonRetrySCProxy");

        boolean isMpDeactivated = carbonLogReader.checkForLog(EXPECTED_MP_DEACTIVATION_MSG, DEFAULT_TIMEOUT);
        Assert.assertTrue(isMpDeactivated, "Message processor hasn't been deactivated upon retries.");

        boolean isRetriedUpon_500_response = carbonLogReader.checkForLog(errorMsg, DEFAULT_TIMEOUT);
        Assert.assertTrue(isRetriedUpon_500_response, "Message processor didn't retry upon failure");

        boolean isRetryCompleted = carbonLogReader.checkForLog(errorMsg, DEFAULT_TIMEOUT, RETRY_COUNT);
        Assert.assertTrue(isRetryCompleted, "Message processor didn't retry " + RETRY_COUNT + " times upon failure");
    }

    @Test(dependsOnMethods = { "testMPRetryUponHTTP_SC_500_response_with_200_And_202_AsNonRetrySCs" },
            description = "Ensure message processor retry with original message envelope "
                    + "https://github.com/wso2/product-ei/issues/4992")
    public void testRetryMessageEnvelope() throws InterruptedException {

        String messageAtBackEnd = "message at 500 backend = " + CLIENT_REQUEST;
        Assert.assertTrue(carbonLogReader.checkForLog(messageAtBackEnd, DEFAULT_TIMEOUT, RETRY_COUNT),
                          "Message processor didn't retry with original message envelope.");
    }

    @Test(dependsOnMethods = { "testRetryMessageEnvelope" },
            description = "test message enveloped stored in fail msg store https://github.com/wso2/product-ei/issues/5072")
    public void testMsgStoredInFailMsgStore() throws Exception {

        clearLogsAndSendRequest("fail-message-store-test-proxy");
        String messageAfterFailOver = "message at 200 backend = " + CLIENT_REQUEST;
        Assert.assertTrue(carbonLogReader.checkForLog(messageAfterFailOver, DEFAULT_TIMEOUT),
                          "Original message envelope is not stored in fail over msg store.");
    }

    private void clearLogsAndSendRequest(String proxyName) throws Exception {

        carbonLogReader.clearLogs();
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-type", "application/xml");
        String inputPayload = "<request>" + CLIENT_REQUEST + "</request>";
        HttpRequestUtil.doPost(new URL(getProxyServiceURLHttp(proxyName)), inputPayload, headers);
    }

    @AfterClass
    private void stop() {
        carbonLogReader.stop();
    }
}
