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
package org.wso2.micro.integrator.utils;

import org.apache.tomcat.dbcp.dbcp2.BasicDataSource;
import org.wso2.micro.integrator.security.user.api.RealmConfiguration;
import org.wso2.micro.integrator.security.user.core.UserStoreException;
import org.wso2.micro.integrator.security.user.core.config.RealmConfigXMLProcessor;
import org.wso2.micro.integrator.security.user.core.jdbc.JDBCUserStoreManager;
import org.wso2.micro.integrator.utils.utils.InputReader;
import org.wso2.micro.integrator.utils.utils.ServerConstants;

import java.io.File;
import java.io.IOException;

/**
 * A tool for updating the password of a user
 */
public class PasswordUpdater {
    private static final String USAGE_MSG_INDENT_SPACES = "         ";
    public static final String DB_URL = "--db-url";
    public static final String DB_DRIVER = "--db-driver";
    public static final String DB_USERNAME = "--db-username";
    public static final String DB_PASSWORD = "--db-password";
    public static final String USERNAME = "--username";
    public static final String NEW_PASSWORD = "--new-password";

    public static final String UPDATE_USER_PASSWORD_SQL = "UPDATE UM_USER SET UM_USER_PASSWORD= ? WHERE UM_USER_NAME= ?";


    public static void main(String[] args) {
        new PasswordUpdater().run(args);
    }

    private void run(String[] args) {
        String wso2wsasHome = System.getProperty(ServerConstants.CARBON_HOME);
        if (wso2wsasHome == null) {
            wso2wsasHome = new File(".").getAbsolutePath();
            System.setProperty(ServerConstants.CARBON_HOME, wso2wsasHome);
        }

        if (args.length == 0) {
            printUsage();
            System.exit(0);
        }

        String dbURL = getParam(DB_URL, args);

        if (dbURL == null || dbURL.indexOf("jdbc:") != 0) {
            System.err.println(" Invalid database DB_URL : " + dbURL);
            printUsage();
        }
        while (dbURL == null || dbURL.trim().length() == 0) {
            System.out.print("DB_URL: ");
            try {
                dbURL = InputReader.readInput();
            } catch (IOException e) {
                System.err.println(" Could not read DB_URL : " + e);
                System.exit(1);
            }
        }

        // ------- DB Connection params
        String dbDriver = getParam(DB_DRIVER, args);
        if (dbDriver == null) {
            System.err.println(" Invalid database driver ");
            printUsage();
        }
        while (dbDriver == null || dbDriver.trim().length() == 0) {
            System.out.print("DB_DRIVER: ");
            try {
                dbDriver = InputReader.readInput();
            } catch (IOException e) {
                System.err.println(" Could not read DB_DRIVER : " + e);
                System.exit(1);
            }
        }
        String dbUsername = getParam(DB_USERNAME, args);
        if (dbUsername == null) {
            System.out.print("Database username: ");
            try {
                dbUsername = InputReader.readInput();
            } catch (IOException e) {
                System.err.println(" Could not read Database username : " + e);
                System.exit(1);
            }
        }
        String dbPassword = getParam(DB_PASSWORD, args);
        if (dbPassword == null) {
            System.out.print("Database password: ");
            try {
                dbPassword = InputReader.readInput();
            } catch (IOException e) {
                System.err.println(" Could not read Database password : " + e);
                System.exit(1);
            }
        }

        // ------------ Load the DB Driver
        try {
            Class.forName(dbDriver);
        } catch (ClassNotFoundException e) {
            System.err.println(" Database driver [" + dbDriver + "] not found in classpath. " + e);
            System.exit(1);
        }

        // --------- Capture the service username and password
        String username = getParam(USERNAME, args);
        while (username == null || username.trim().length() == 0) {
            System.out.print("Username: ");
            try {
                username = InputReader.readInput();
            } catch (IOException e) {
                System.err.println(" Could not read username : " + e);
                System.exit(1);
            }
        }

        String password = getParam(NEW_PASSWORD, args);
        if (password == null || password.trim().length() == 0) {
            String passwordRepeat = null;
            while (password == null || password.trim().length() == 0) {
                try {
                    password = InputReader.readPassword("New password: ");
                } catch (IOException e) {
                    System.err.println("Unable to read password : " + e);
                    System.exit(1);
                }
            }
            while (passwordRepeat == null || passwordRepeat.trim().length() == 0) {
                try {
                    passwordRepeat = InputReader.readPassword("Re-enter new password: ");
                } catch (IOException e) {
                    System.err.println("Unable to read re-entered password : " + e);
                    System.exit(1);
                }
            }
            if (!password.equals(passwordRepeat)) {
                System.err.println(" Password and re-entered password do not match");
                System.exit(1);
            }
        }

        // DataSource is created to connect to user store DB using input parameters given by user

        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(dbURL);
        ds.setDriverClassName(dbDriver);
        ds.setUsername(dbUsername);
        ds.setPassword(dbPassword);

        try {
            RealmConfiguration realmConfig = new RealmConfigXMLProcessor()
                    .buildRealmConfigurationFromFile();
            JDBCUserStoreManager userStore = new JDBCUserStoreManager(ds, realmConfig);
            userStore.doUpdateCredentialByAdmin(username, password);
            System.out.println("Password updated successfully.");
        } catch (UserStoreException ex) {
            System.err.println("Error updating credentials for user " + username + " : " + ex);
        }
    }

    /**
     * This will check the given parameter in the array and will return, if available
     *
     * @param param
     * @param args
     * @return the parameter
     */
    private String getParam(String param, String[] args) {
        if (param == null || "".equals(param)) {
            return null;
        }
        for (int i = 0; i < args.length; i = i + 2) {
            String arg = args[i];
            if (param.equalsIgnoreCase(arg) && (args.length >= (i + 1))) {
                return args[i + 1];
            }
        }
        return null;
    }

    private void printUsage() {
        System.out.println("Usage: chpasswd --db-url DB_URL [OPTIONS]\n");
        System.out.println(USAGE_MSG_INDENT_SPACES + DB_URL + " : The JDBC database URL. " +
                "e.g. jdbc:mysql://localhost:3306/userdb\n");
        System.out.println("Options");
        System.out.println(USAGE_MSG_INDENT_SPACES + DB_DRIVER + "    : The database driver class. " +
                "e.g. com.mysql.jdbc.Driver");
        System.out.println(USAGE_MSG_INDENT_SPACES + DB_USERNAME + "  : The database username");
        System.out.println(USAGE_MSG_INDENT_SPACES + DB_PASSWORD + "  : The database password");
        System.out.println(USAGE_MSG_INDENT_SPACES + USERNAME + "     : The username of the user whose " +
                "password is to be changed. If this is not given, " +
                "you will be prompted for this field later.");
        System.out.println(USAGE_MSG_INDENT_SPACES + NEW_PASSWORD + " : The new password of the user " +
                "whose password is to be changed. If this is not given, " +
                "you will be prompted for this field later.");
    }
}