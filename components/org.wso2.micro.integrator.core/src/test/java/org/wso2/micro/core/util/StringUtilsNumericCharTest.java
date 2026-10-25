/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC licenses this file to you under the Apache License,
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
package org.wso2.micro.core.util;

import org.junit.Test;
import org.wso2.micro.core.util.xml.XMLPrettyPrinter;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilderFactory;

import org.xml.sax.InputSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for hexadecimal numeric character references used by XML formatting.
 */
public class StringUtilsNumericCharTest {

    @Test
    public void escapePreservesAsciiAndEncodesCodePoints() throws Exception {
        assertEquals("", StringUtils.escapeNumericChar(""));
        assertEquals("ASCII <&> 123", StringUtils.escapeNumericChar("ASCII <&> 123"));
        assertEquals("&#xE9;&#x4E2D;&#x1F600;&#x20000;",
                StringUtils.escapeNumericChar("é中😀𠀀"));
        StringWriter writer = new StringWriter();
        StringUtils.escapeNumericChar(writer, "é😀");
        assertEquals("&#xE9;&#x1F600;", writer.toString());
    }

    @Test
    public void escapedSupplementaryCharactersAreValidXml() throws Exception {
        String original = "é中😀𠀀";
        String xml = "<value>" + StringUtils.escapeNumericChar(original) + "</value>";
        assertEquals(original, DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(new InputSource(new StringReader(xml))).getDocumentElement().getTextContent());
    }

    @Test
    public void unescapeAcceptsVariableLengthHexReferences() throws Exception {
        assertEquals("Aé中😀𠀀", StringUtils.unescapeNumericChar("&#x41;&#xE9;&#x4e2d;&#x1F600;&#x20000;"));
        assertEquals("é", StringUtils.unescapeNumericChar("&#x0000E9;"));
        StringWriter writer = new StringWriter();
        StringUtils.unescapeNumericChar(writer, "before &#xE9;&#x1F600; after");
        assertEquals("before é😀 after", writer.toString());
    }

    @Test
    public void numericReferencesRoundTrip() {
        for (String value : new String[]{"", "ASCII", "café", "中", "😀𠀀", "Aé中😀Z"}) {
            assertEquals(value, StringUtils.unescapeNumericChar(StringUtils.escapeNumericChar(value)));
        }
    }

    @Test
    public void unescapePreservesMalformedAndNonScalarReferences() {
        for (String value : new String[]{"&", "&#", "&#x", "&#x1234", "&#x;", "&#xZZZZ;",
                "&#x+041;", "&#x-041;", "&#xD800;", "&#xDFFF;", "&#x110000;",
                "&#xFFFFFFFFFFFFFFFF;", "&amp;", "&#233;"}) {
            assertEquals(value, StringUtils.unescapeNumericChar(value));
        }
        assertEquals("&#xZZZZ;é", StringUtils.unescapeNumericChar("&#xZZZZ;&#xE9;"));
    }

    @Test
    public void nullInputDoesNotWriteAnything() throws Exception {
        assertNull(StringUtils.escapeNumericChar(null));
        assertNull(StringUtils.unescapeNumericChar(null));
        StringWriter writer = new StringWriter();
        StringUtils.escapeNumericChar(writer, null);
        StringUtils.unescapeNumericChar(writer, null);
        assertEquals("", writer.toString());
    }

    @Test
    public void prettyPrinterUsesNumericCodecForBothModes() throws Exception {
        String text = "é中😀𠀀";
        String xml = "<value>" + text + "</value>";
        XMLPrettyPrinter encoder = new XMLPrettyPrinter(new ByteArrayInputStream(
                xml.getBytes(StandardCharsets.UTF_8)), true, true, "UTF-8");
        String encoded = encoder.xmlFormat();
        assertEquals(text, DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(new InputSource(new StringReader(encoded))).getDocumentElement().getTextContent());

        XMLPrettyPrinter decoder = new XMLPrettyPrinter(new ByteArrayInputStream(
                "<value>&#xE9;&#x4E2D;&#x1F600;&#x20000;</value>".getBytes(StandardCharsets.UTF_8)));
        String decoded = decoder.xmlFormat();
        assertTrue(decoded.contains(text));
        assertEquals(text, DocumentBuilderFactory.newInstance().newDocumentBuilder()
                .parse(new InputSource(new StringReader(decoded))).getDocumentElement().getTextContent());
    }
}
