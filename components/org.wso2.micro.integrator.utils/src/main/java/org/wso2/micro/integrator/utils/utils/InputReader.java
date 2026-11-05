/*
 *  Copyright (c) 2023, WSO2 LLC (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.wso2.micro.integrator.utils.utils;

import java.io.PrintWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * A script for getting user input for any text and password
 */
public class InputReader {
    private InputReader() {
        //disable external instantiation
    }

    private static BufferedReader stdinReader;

    public static String readInput() throws IOException {
        // Read one line per prompt so that the remaining input is left for the next prompt
        String line = getStdinReader().readLine();
        if (line == null) {
            throw new EOFException("End of input reached");
        }
        return line.trim();
    }

    public static String readPassword(String prompt) throws IOException {
        // Standard input and output are not closed so that subsequent prompts can still use them
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out), true);
        String password = new PasswordPrompt(prompt, out).getPassword(getStdinReader());
        if (password == null) {
            throw new EOFException("End of input reached");
        }
        return password;
    }

    private static synchronized BufferedReader getStdinReader() {
        if (stdinReader == null) {
            stdinReader = new BufferedReader(new InputStreamReader(System.in));
        }
        return stdinReader;
    }
}
