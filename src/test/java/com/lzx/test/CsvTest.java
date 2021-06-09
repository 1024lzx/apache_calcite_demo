/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lzx.test;

import org.apache.calcite.util.Sources;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Unit test of the Calcite adapter for CSV.
 */
public class CsvTest {
    @Test
    public void testSelect() throws SQLException {
        sql("model", "select * from EMPS").ok();
    }

    @Test
    public void testSelectSingleProjectGz() throws SQLException {
        sql("smart", "select name from EMPS").ok();
    }

    private Fluent sql(String model, String sql) {
        return new Fluent(model, sql, this::output);
    }

    private void checkSql(String sql, String model, Consumer<ResultSet> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            fn.accept(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }


    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        return Sources.of(CsvTest.class.getResource("/calcite/" + path)).file().getAbsolutePath();
    }

    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {
        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;

        Fluent(String model, String sql, Consumer<ResultSet> expect) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql(sql, model, expect);
                return this;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
    }
}
