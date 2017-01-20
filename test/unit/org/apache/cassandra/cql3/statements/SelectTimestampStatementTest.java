/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.statements;

import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

/**
 * @author Wenfeng Zhuo (wz2366@columbia.edu)
 * @createAt 01-18-2017
 */
public class SelectTimestampStatementTest extends CQLTester
{

    @Test
    public void testOnSinglePartitionKey() throws Throwable
    {
        int size = 10;

        createTable("CREATE TABLE %s (k text, t int, v text, PRIMARY KEY (k, t));");

        long expectedMaxTimestamp = Long.MIN_VALUE;
        Random r = new Random();
        for (int i = 0; i < size; i ++) {
            long timestamp = r.nextLong();
            timestamp = timestamp < 0 ? -(timestamp + 1) : timestamp;
            expectedMaxTimestamp = Math.max(expectedMaxTimestamp, timestamp);
            execute("INSERT INTO %s (k, t, v) values (?, ?, ?) USING TIMESTAMP ?", "key", i, "v11", timestamp);
        }

        flush();

        assertRows(execute("SELECT MAX TIMESTAMP %s (?)", "key"),
            row(expectedMaxTimestamp)
        );
    }

    @Test
    public void testOnMultiplePartitionKeys() throws Throwable
    {
        int size = 10;

        String key = "key";
        int t = 0;

        createTable("CREATE TABLE %s (k1 text, k2 int, v text, PRIMARY KEY ((k1, k2)));");

        long expectedMaxTimestamp = Long.MIN_VALUE;
        Random r = new Random();
        for (int i = 0; i < size; i ++) {
            long timestamp = r.nextLong();
            timestamp = timestamp < 0 ? -(timestamp + 1) : timestamp;
            expectedMaxTimestamp = Math.max(expectedMaxTimestamp, timestamp);
            execute("INSERT INTO %s (k1, k2, v) values (?, ?, ?) USING TIMESTAMP ?", key, t, "v11", timestamp);
        }

        flush();

        assertRows(execute("SELECT MAX TIMESTAMP %s (?, ?)", key, t),
            row(expectedMaxTimestamp)
        );
    }

    @Test
    public void testOnNoResultReturned() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 text, t int, v text, PRIMARY KEY (k1));");

        assertEmpty(execute("SELECT MAX TIMESTAMP %s (?)", "key"));
    }

}
