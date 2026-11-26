/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.core.util;
/*
 * 
 */

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class StringUtils {
    private StringUtils() {
    }

    /**
     * An empty immutable <code>String</code> array.
     */
    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * Tests if this string starts with the specified prefix (Ignoring whitespaces)
     *
     * @param prefix
     * @param string
     * @return boolean
     */
    public static boolean startsWithIgnoreWhitespaces(String prefix, String string) {
        int index1 = 0;
        int index2 = 0;
        int length1 = prefix.length();
        int length2 = string.length();
        char ch1 = ' ';
        char ch2 = ' ';
        while (index1 < length1 && index2 < length2) {
            while (index1 < length1 && Character.isWhitespace(ch1 = prefix.charAt(index1))) {
                index1++;
            }
            while (index2 < length2 && Character.isWhitespace(ch2 = string.charAt(index2))) {
                index2++;
            }
            if (index1 == length1 && index2 == length2) {
                return true;
            }
            if (ch1 != ch2) {
                return false;
            }
            index1++;
            index2++;
        }
        
        if (index1 < length1 && index2 >= length2) {
            return false;
        }
        
        return true;
    }

    /**
     * <p>Splits the provided text into an array, separator specified.
     * This is an alternative to using StringTokenizer.</p>
     * <p/>
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as one separator.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     * <p/>
     * <pre>
     * StringUtils.split(null, *)         = null
     * StringUtils.split("", *)           = []
     * StringUtils.split("a.b.c", '.')    = ["a", "b", "c"]
     * StringUtils.split("a..b.c", '.')   = ["a", "b", "c"]
     * StringUtils.split("a:b:c", '.')    = ["a:b:c"]
     * StringUtils.split("a\tb\nc", null) = ["a", "b", "c"]
     * StringUtils.split("a b c", ' ')    = ["a", "b", "c"]
     * </pre>
     *
     * @param str           the String to parse, may be null
     * @param separatorChar the character used as the delimiter,
     *                      <code>null</code> splits on whitespace
     * @return an array of parsed Strings, <code>null</code> if null String input
     */
    public static String[] split(String str, char separatorChar) {
        if (str == null) {
            return null;
        }
        int len = str.length();
        if (len == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List list = new ArrayList();
        int i = 0, start = 0;
        boolean match = false;
        while (i < len) {
            if (str.charAt(i) == separatorChar) {
                if (match) {
                    list.add(str.substring(start, i));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(str.substring(start, i));
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    // Empty checks
    //-----------------------------------------------------------------------

    /**
     * <p>Checks if a String is empty ("") or null.</p>
     * <p/>
     * <pre>
     * StringUtils.isEmpty(null)      = true
     * StringUtils.isEmpty("")        = true
     * StringUtils.isEmpty(" ")       = false
     * StringUtils.isEmpty("bob")     = false
     * StringUtils.isEmpty("  bob  ") = false
     * </pre>
     * <p/>
     * <p>NOTE: This method changed in Lang version 2.0.
     * It no longer trims the String.
     * That functionality is available in isBlank().</p>
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is empty or null
     */
    public static boolean isEmpty(String str) {
        return (str == null || str.length() == 0);
    }

    // Stripping
    //-----------------------------------------------------------------------

    /**
     * <p>Strips whitespace from the start and end of a String.</p>
     * <p/>
     * <p>This removes whitespace. Whitespace is defined by
     * {@link Character#isWhitespace(char)}.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     * <p/>
     * <pre>
     * StringUtils.strip(null)     = null
     * StringUtils.strip("")       = ""
     * StringUtils.strip("   ")    = ""
     * StringUtils.strip("abc")    = "abc"
     * StringUtils.strip("  abc")  = "abc"
     * StringUtils.strip("abc  ")  = "abc"
     * StringUtils.strip(" abc ")  = "abc"
     * StringUtils.strip(" ab c ") = "ab c"
     * </pre>
     *
     * @param str the String to remove whitespace from, may be null
     * @return the stripped String, <code>null</code> if null String input
     */
    public static String strip(String str) {
        return strip(str, null);
    }

    /**
     * <p>Strips any of a set of characters from the start and end of a String.
     * This is similar to {@link String#trim()} but allows the characters
     * to be stripped to be controlled.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.
     * An empty string ("") input returns the empty string.</p>
     * <p/>
     * <p>If the stripChars String is <code>null</code>, whitespace is
     * stripped as defined by {@link Character#isWhitespace(char)}.
     * Alternatively use {@link #strip(String)}.</p>
     * <p/>
     * <pre>
     * StringUtils.strip(null, *)          = null
     * StringUtils.strip("", *)            = ""
     * StringUtils.strip("abc", null)      = "abc"
     * StringUtils.strip("  abc", null)    = "abc"
     * StringUtils.strip("abc  ", null)    = "abc"
     * StringUtils.strip(" abc ", null)    = "abc"
     * StringUtils.strip("  abcyx", "xyz") = "  abc"
     * </pre>
     *
     * @param str        the String to remove characters from, may be null
     * @param stripChars the characters to remove, null treated as whitespace
     * @return the stripped String, <code>null</code> if null String input
     */
    public static String strip(String str, String stripChars) {
        if (str == null) {
            return str;
        }
        int len = str.length();
        if (len == 0) {
            return str;
        }
        int start = getStripStart(str, stripChars);
        if (start == len) {
            return "";
        }
        int end = getStripEnd(str, stripChars);
        return (start == 0 && end == len) ? str : str.substring(start, end);
    }

    /**
     * <p>Strips any of a set of characters from the start of a String.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.
     * An empty string ("") input returns the empty string.</p>
     * <p/>
     * <p>If the stripChars String is <code>null</code>, whitespace is
     * stripped as defined by {@link Character#isWhitespace(char)}.</p>
     * <p/>
     * <pre>
     * StringUtils.stripStart(null, *)          = null
     * StringUtils.stripStart("", *)            = ""
     * StringUtils.stripStart("abc", "")        = "abc"
     * StringUtils.stripStart("abc", null)      = "abc"
     * StringUtils.stripStart("  abc", null)    = "abc"
     * StringUtils.stripStart("abc  ", null)    = "abc  "
     * StringUtils.stripStart(" abc ", null)    = "abc "
     * StringUtils.stripStart("yxabc  ", "xyz") = "abc  "
     * </pre>
     *
     * @param str        the String to remove characters from, may be null
     * @param stripChars the characters to remove, null treated as whitespace
     * @return the stripped String, <code>null</code> if null String input
     */
    public static String stripStart(String str, String stripChars) {
        int start = getStripStart(str, stripChars);
        return (start <= 0) ? str : str.substring(start);
    }

    private static int getStripStart(String str, String stripChars) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return -1;
        }
        int start = 0;
        if (stripChars == null) {
            while ((start != strLen) && Character.isWhitespace(str.charAt(start))) {
                start++;
            }
        } else if (stripChars.length() == 0) {
            return start;
        } else {
            while ((start != strLen) && (stripChars.indexOf(str.charAt(start)) != -1)) {
                start++;
            }
        }
        return start;
    }

    /**
     * <p>Strips any of a set of characters from the end of a String.</p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.
     * An empty string ("") input returns the empty string.</p>
     * <p/>
     * <p>If the stripChars String is <code>null</code>, whitespace is
     * stripped as defined by {@link Character#isWhitespace(char)}.</p>
     * <p/>
     * <pre>
     * StringUtils.stripEnd(null, *)          = null
     * StringUtils.stripEnd("", *)            = ""
     * StringUtils.stripEnd("abc", "")        = "abc"
     * StringUtils.stripEnd("abc", null)      = "abc"
     * StringUtils.stripEnd("  abc", null)    = "  abc"
     * StringUtils.stripEnd("abc  ", null)    = "abc"
     * StringUtils.stripEnd(" abc ", null)    = " abc"
     * StringUtils.stripEnd("  abcyx", "xyz") = "  abc"
     * </pre>
     *
     * @param str        the String to remove characters from, may be null
     * @param stripChars the characters to remove, null treated as whitespace
     * @return the stripped String, <code>null</code> if null String input
     */
    public static String stripEnd(String str, String stripChars) {
        int end = getStripEnd(str, stripChars);
        return (end < 0) ? str : str.substring(0, end);
    }

    private static int getStripEnd(String str, String stripChars) {
        int end;
        if (str == null || (end = str.length()) == 0) {
            return -1;
        }
        if (stripChars == null) {
            while ((end != 0) && Character.isWhitespace(str.charAt(end - 1))) {
                end--;
            }
        } else if (stripChars.length() == 0) {
            return end;
        } else {
            while ((end != 0) && (stripChars.indexOf(str.charAt(end - 1)) != -1)) {
                end--;
            }
        }
        return end;
    }

    /**
     * write the escaped version of a given string
     *
     * @param str string to be encoded
     * @return a new escaped <code>String</code>, <code>null</code> if null string input
     */
    public static String escapeNumericChar(String str) {
        if (str == null) {
            return null;
        }
        try {
            StringWriter writer = new StringWriter(str.length());
            escapeNumericChar(writer, str);
            return writer.toString();
        } catch (IOException ioe) {
            // this should never ever happen while writing to a StringWriter
            ioe.printStackTrace();
            return null;
        }
    }

    /**
     * write the escaped version of a given string
     *
     * @param out writer to write this string to
     * @param str string to be encoded
     */
    public static void escapeNumericChar(Writer out, String str)
            throws IOException {
        if (str == null) {
            return;
        }
        int length = str.length();
        for (int i = 0; i < length;) {
            int character = str.codePointAt(i);
            if (character > 0x7F) {
                out.write("&#x");
                out.write(Integer.toHexString(character).toUpperCase());
                out.write(";");
            } else {
                out.write(character);
            }
            i += Character.charCount(character);
        }
    }

    /**
     * <p>This method provides a way to convert the a QName to local name. This need
     * to be done is because Axis 1.x follow the java naming convetation for local name
     * rather then xml naming convenation for the local name. </p>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     * <p/>
     * <pre>
     * 	makeQNameToMatchLocalName("Foo.bar")  = fooBar
     * 	makeQNameToMatchLocalName("Foo.Bar")  = fooBar
     * 	makeQNameToMatchLocalName("FooBar")   = fooBar
     * 	makeQNameToMatchLocalName("Foobar")   = fooBar
     * 	makeQNameToMatchLocalName("fooBar")   = fooBar
     * 	makeQNameToMatchLocalName("foobar")   = foobar
     * 	makeQNameToMatchLocalName("Foo:bar")  = fooBar
     * 	makeQNameToMatchLocalName("Foo-Bar")  = fooBar
     * 	makeQNameToMatchLocalName("Foo-bar")  = fooBar
     * 	makeQNameToMatchLocalName("Foo_bar")  = foo_bar
     * 	makeQNameToMatchLocalName("Foo_Bar")  = foo_Bar
     * 	makeQNameToMatchLocalName("foo:bar")  = fooBar
     * </pre>
     *
     * @param str the String to parse, may be null
     *            <code>null</code> splits on whitespace
     * @return String the java naming compliante name
     */
    public static String makeQNameToMatchLocalName(String str) {

        if (str == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer(str);

        char firstChar = sb.charAt(0);

        if (Character.isUpperCase(firstChar)) {
            sb.setCharAt(0, Character.toLowerCase(firstChar));
        }
        int iIndex = str.indexOf('.');

        if (iIndex == 0) {
            sb.deleteCharAt(0);

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf(':');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf('-');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }
        } else if (iIndex > 0) {
            str = deleteCharAndChangeNextCharToUpperCase(sb.toString(), ".");
            sb.delete(0, sb.length());
            sb.append(str);
        }

        iIndex = str.indexOf(':');
        if (iIndex == 0) {
            sb.deleteCharAt(0);

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf('.');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf('-');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }

        } else if (iIndex > 0) {
            str = deleteCharAndChangeNextCharToUpperCase(sb.toString(), ":");
            sb.delete(0, sb.length());
            sb.append(str);
        }

        iIndex = str.indexOf('-');
        if (iIndex == 0) {
            sb.deleteCharAt(0);

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf('.');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }

            if (sb.length() > 0) {
                iIndex = sb.toString().indexOf(':');
                if (iIndex == 0) {
                    str = makeQNameToMatchLocalName(sb.toString());
                    sb.delete(0, sb.length());
                    sb.append(str);
                }
            }
        } else if (iIndex > 0) {
            str = deleteCharAndChangeNextCharToUpperCase(sb.toString(), "-");
            sb.delete(0, sb.length());
            sb.append(str);
        }

        if (sb.length() > 0) {
            firstChar = sb.charAt(0);

            if (Character.isUpperCase(firstChar)) {
                sb.setCharAt(0, Character.toLowerCase(firstChar));
            }
        }

        return isEmpty(sb.toString()) ? null : sb.toString();
    }

    /**
     * <p>This method provided a way to delete the given string and change the
     * following first character to capital letter, this is provided because
     * Axis 1.x follow the java naming convenation rather then xml naming convention
     * for there local name.</p>
     * <p/>
     * <p/>
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     * <p/>
     * <pre>
     * 	deleteCharAndChangeNextCharToUpperCase("Foo.bar") = fooBar
     *  deleteCharAndChangeNextCharToUpperCase("Foo.Bar") = fooBar
     *  deleteCharAndChangeNextCharToUpperCase("Foo:bar") = fooBar
     *  deleteCharAndChangeNextCharToUpperCase("Foo-Bar") = fooBar
     * 	deleteCharAndChangeNextCharToUpperCase("Foo-bar") = fooBar
     * 	deleteCharAndChangeNextCharToUpperCase("Foo_bar") = foo_bar
     * 	deleteCharAndChangeNextCharToUpperCase("Foo_Bar") = foo_Bar
     * 	deleteCharAndChangeNextCharToUpperCase("Foo_bar") = foo_bar
     * 	deleteCharAndChangeNextCharToUpperCase("foo:bar") = fooBar
     * </pre>
     *
     * @param psWord    the String to be changed
     *                  <code>return null</code> if null passed.
     * @param psReplace the String that need to be eliminated,
     *                  <code>return null or psWord if psWord is not null</code>
     *                  if null passed.
     * @return String the java naming compliant name
     */
    public static String deleteCharAndChangeNextCharToUpperCase(String psWord, String psReplace) {
        if (psWord == null) {
            return null;
        }

        if (psReplace == null) {
            return psWord;
        }

        StringBuffer lsNewStr = new StringBuffer();

        int liFound = 0;
        int liLastPointer = 0;
        boolean skipNextChar = false;

        do {
            liFound = psWord.indexOf(psReplace, liLastPointer);
            if (liFound < 0) {
                if (!skipNextChar) {
                    lsNewStr.append(psWord
                            .substring(liLastPointer, psWord.length()));
                } else {
                    lsNewStr.append(psWord
                            .substring(liLastPointer + 1, psWord.length()));
                    skipNextChar = false;
                }
            } else {
                char nextChar = 0;
                if ((liFound + psReplace.length()) < psWord.length()) {
                    nextChar = psWord.charAt(liFound + psReplace.length());
                }
                if (liFound > liLastPointer && nextChar > 0) {
                    lsNewStr.append(psWord.substring(liLastPointer, (liFound)));
                    if (!Character.isUpperCase(nextChar)) {
                        lsNewStr.append(Character.toUpperCase(nextChar));
                    } else {
                        lsNewStr.append(nextChar);
                    }
                }
                liLastPointer = liFound + psReplace.length();
                skipNextChar = true;
            }
        } while (liFound > -1);

        return lsNewStr.toString();
    }

    /**
     * <p>Unescapes numeric character referencs found in the <code>String</code>.</p>
     * <p/>
     * <p>For example, it will return a unicode string which means the specified numeric
     * character references looks like "&#x3088;&#x3046;&#x3053;&#x305d;".</p>
     *
     * @param str the <code>String</code> to unescape, may be null
     * @return a new unescaped <code>String</code>, <code>null</code> if null string input
     */
    public static String unescapeNumericChar(String str) {
        if (str == null) {
            return null;
        }
        try {
            StringWriter writer = new StringWriter(str.length());
            unescapeNumericChar(writer, str);
            return writer.toString();
        } catch (IOException ioe) {
            // this should never ever happen while writing to a StringWriter
            ioe.printStackTrace();
            return null;
        }
    }

    /**
     * <p>Unescapes numeric character references found in the <code>String</code> to a
     * <code>Writer</code>.</p>
     * <p/>
     * <p>For example, it will return a unicode string which means the specified numeric
     * character references looks like "&#x3088;&#x3046;&#x3053;&#x305d;".</p>
     * <p/>
     * <p>A <code>null</code> string input has no effect.</p>
     * <p>Hexadecimal references may contain any number of digits. Malformed references and
     * references outside the Unicode scalar range are left unchanged.</p>
     *
     * @param out the <code>Writer</code> used to output unescaped characters
     * @param str the <code>String</code> to unescape, may be null
     * @throws IllegalArgumentException if the Writer is <code>null</code>
     * @throws IOException      if error occurs on underlying Writer
     */
    public static void unescapeNumericChar(Writer out, String str) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("The Writer must not be null");
        }
        if (str == null) {
            return;
        }

        int sz = str.length();
        for (int i = 0; i < sz; i++) {
            if (str.startsWith("&#x", i)) {
                int end = i + 3;
                while (end < sz) {
                    char digit = str.charAt(end);
                    if ((digit >= '0' && digit <= '9') || (digit >= 'a' && digit <= 'f') ||
                            (digit >= 'A' && digit <= 'F')) {
                        end++;
                    } else {
                        break;
                    }
                }
                if (end > i + 3 && end < sz && str.charAt(end) == ';') {
                    try {
                        int value = Integer.parseInt(str.substring(i + 3, end), 16);
                        if (Character.isValidCodePoint(value) &&
                                (value < Character.MIN_SURROGATE || value > Character.MAX_SURROGATE)) {
                            out.write(Character.toChars(value));
                            i = end;
                            continue;
                        }
                    } catch (NumberFormatException nfe) {
                        // Preserve references whose value cannot fit in a Unicode code point.
                    }
                }
            }
            out.write(str.charAt(i));
        }
    }
}
