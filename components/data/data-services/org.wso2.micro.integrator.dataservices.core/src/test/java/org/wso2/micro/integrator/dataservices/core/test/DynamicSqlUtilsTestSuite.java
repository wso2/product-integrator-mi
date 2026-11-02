/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
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

package org.wso2.micro.integrator.dataservices.core.test;

import junit.framework.TestCase;
import org.wso2.micro.integrator.dataservices.core.script.DynamicSqlUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests SQL generated from ordered table metadata by the data service generator.
 */
public class DynamicSqlUtilsTestSuite extends TestCase {

    private final DynamicSqlUtils sqlUtils = new DynamicSqlUtils();

    private static final String[][] SCHEMAS_AND_TABLES = {
            {null, "employees"}, {"", "employees"}, {"hr", "hr.employees"}
    };

    public void testSelectAllPreservesColumnsAndQualifiesTable() {
        for (String[] schemaAndTable : SCHEMAS_AND_TABLES) {
            assertEquals("SELECT employee_id, display_name FROM " + schemaAndTable[1],
                    sqlUtils.getSelectAll("  employees  ", schemaAndTable[0], " employee_id, display_name "));
        }
    }

    public void testSelectByKeyRetainsWherePlaceholder() {
        for (String[] schemaAndTable : SCHEMAS_AND_TABLES) {
            assertEquals("SELECT display_name, employee_id FROM " + schemaAndTable[1] + " WHERE employee_id=?",
                    sqlUtils.getSelectByKey(" employees ", schemaAndTable[0], "employee_id",
                            " display_name, employee_id "));
        }
    }

    public void testInsertPreservesColumnOrderAndPlaceholderCount() {
        List<String> columns = Arrays.asList("display_name", "department", "employee_id");
        for (String[] schemaAndTable : SCHEMAS_AND_TABLES) {
            String statement = sqlUtils.getInsertStatement(" employees ", schemaAndTable[0], columns);
            assertEquals("INSERT INTO " + schemaAndTable[1] +
                    "(display_name,department,employee_id) VALUES(?,?,?)", statement);
            assertEquals(columns.size(), countPlaceholders(statement));
        }
    }

    public void testInsertWithSingleColumn() {
        String statement = sqlUtils.getInsertStatement("employees", "hr", Arrays.asList("display_name"));
        assertEquals("INSERT INTO hr.employees(display_name) VALUES(?)", statement);
        assertEquals(1, countPlaceholders(statement));
    }

    public void testUpdateExcludesKeyFromSetAndPreservesInputOrder() {
        List<String> columns = new ArrayList<>(Arrays.asList("display_name", "department", "employee_id"));
        List<String> original = new ArrayList<>(columns);
        for (String[] schemaAndTable : SCHEMAS_AND_TABLES) {
            String statement = sqlUtils.getUpdateStatement(" employees ", schemaAndTable[0], columns, "employee_id");
            assertEquals("UPDATE " + schemaAndTable[1] +
                    " SET display_name=?,department=? WHERE employee_id=?", statement);
            assertEquals(columns.size(), countPlaceholders(statement));
            assertEquals(original, columns);
        }
    }

    public void testUpdateWithSingleNonKeyColumn() {
        String statement = sqlUtils.getUpdateStatement("employees", "hr",
                Arrays.asList("display_name", "employee_id"), "employee_id");
        assertEquals("UPDATE hr.employees SET display_name=? WHERE employee_id=?", statement);
        assertEquals(2, countPlaceholders(statement));
    }

    public void testDeleteQualifiesTableAndRetainsKeyPlaceholder() {
        for (String[] schemaAndTable : SCHEMAS_AND_TABLES) {
            assertEquals("DELETE FROM " + schemaAndTable[1] + " WHERE employee_id=?",
                    sqlUtils.getDeleteStatement(" employees ", schemaAndTable[0], "employee_id"));
        }
    }

    private int countPlaceholders(String statement) {
        int count = 0;
        for (int i = 0; i < statement.length(); i++) {
            if (statement.charAt(i) == '?') {
                count++;
            }
        }
        return count;
    }
}
