/**
 * Copyright (c) 2009, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.micro.integrator.registry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 */
public class RegistryHelper {

    private static final Log log = LogFactory.getLog(RegistryHelper.class);
    public static final char URL_SEPARATOR_CHAR = '/';

    public static File getRootDirectory(String path) {

        if (path == null) {
            String msg = "Path can not be null";
            log.error(msg);
            throw new RuntimeException(msg);
        }

        try {
            URL url = new URL(path);
            path = url.getPath();
        } catch (MalformedURLException e) {
            // don't do any thing if this is not a valid URL
        }

        File file = new File(path);
        if (!file.isAbsolute()) {
            String esbHome = getHome();
            if (!esbHome.endsWith("/")) {
                esbHome = esbHome + "/";
            }
            file = new File(esbHome + path);
        }
        return file;
    }

    public static String getHome() {
        String carbonHome = System.getProperty("carbon.home");
        if (carbonHome == null || "".equals(carbonHome) || ".".equals(carbonHome)) {
            carbonHome = getSystemDependentPath(new File(".").getAbsolutePath());
        }
        return carbonHome;
    }

    public static String getSystemDependentPath(String path) {
        return path.replace(URL_SEPARATOR_CHAR, File.separatorChar);
    }

    /**
     * Check whether the directory is empty.
     * <p>
     * Returns false when the path is not an existing directory or when its content cannot be
     * listed, which includes the case where the directory was removed concurrently. Callers use
     * this method to decide whether a directory should be cleaned up, so reporting false for a
     * directory that is already gone correctly skips that cleanup.
     *
     * @param directory directory path
     */
    public static boolean isDirectoryEmpty (String directory) {
        File file = new File(directory);
        if (file.isDirectory()) {
            String[] children = file.list();
            if (children == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to list the contents of " + directory
                        + ". It may have been removed concurrently.");
                }
                return false;
            }
            return children.length == 0;
        } else {
            return false;
        }
    }

}
