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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Validation;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 *
 * @author Wenfeng Zhuo (wz2366@columbia.edu)
 * @createAt 01-18-2017
 */
public class SelectTimestampStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectTimestampStatement.class);

    public CFMetaData cfm;

    public StatementRestrictions restrictions;

    public SelectTimestampStatement(CFMetaData cfm, StatementRestrictions restrictions) {
        this.cfm = cfm;
        this.restrictions = restrictions;
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {

    }

    public void validate(ClientState state) throws RequestValidationException
    {

    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        List<SinglePartitionReadCommand> commands = new ArrayList<>();
        for (ByteBuffer key : keys) {
            QueryProcessor.validateKey(key);
            DecoratedKey decoratedKey = cfm.decorateKey(ByteBufferUtil.clone(key));
            ClusteringIndexFilter clusteringIndexFilter = new ClusteringIndexNamesFilter(restrictions.getClusteringColumns(options), false);
            commands.add(SinglePartitionReadCommand.create(cfm, FBUtilities.nowInSeconds(), decoratedKey, ColumnFilter.selectionBuilder().build(), clusteringIndexFilter));
        }
        ReadQuery partitionRead = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);

        try (PartitionIterator data = partitionRead.execute(options.getConsistency(), state.getClientState(), queryStartNanoTime)) {
            return processResult(data);
        }
    }

    private ResultMessage processResult(PartitionIterator data) {
        long maxTimestamp = Long.MIN_VALUE;
        while (data.hasNext()) {
            RowIterator rows = data.next();
            while (rows.hasNext()) {
                Row row = rows.next();
                maxTimestamp = Math.max(maxTimestamp, row.primaryKeyLivenessInfo().timestamp());
            }
        }
        List<ColumnSpecification> colums = Arrays.asList(new ColumnSpecification(cfm.ksName, cfm.cfName, new ColumnIdentifier("timestamp", false), LongType.instance));
        ResultSet resultSet = new ResultSet(colums);
        resultSet.addRow(Arrays.asList(ByteBufferUtil.bytes(maxTimestamp)));
        return new ResultMessage.Rows(resultSet);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        return execute(state, options, System.nanoTime());
    }

    public Iterable<Function> getFunctions()
    {
        return new ArrayList<>();
    }

    public static class RawStatement extends CFStatement
    {

        public List<Term.Raw> values;

        public RawStatement(CFName cfName, List<Term.Raw> values)
        {
            super(cfName);
            this.values = values;
        }

        public Prepared prepare() throws RequestValidationException
        {
            CFMetaData cfm = Validation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            WhereClause.Builder whereClause = new WhereClause.Builder();

            List<ColumnDefinition> partitionKeys = cfm.partitionKeyColumns();
            for (int i = 0; i < values.size(); i ++) {
                whereClause.add(
                    new SingleColumnRelation(
                        ColumnDefinition.Raw.forColumn(partitionKeys.get(i)),
                        Operator.EQ,
                        values.get(i)
                    )
                );
            }

            StatementRestrictions restrictions = new StatementRestrictions(StatementType.SELECT,
                                                                           cfm,
                                                                           whereClause.build(),
                                                                           boundNames,
                                                                           false,
                                                                           false,
                                                                           false);
            SelectTimestampStatement stmt = new SelectTimestampStatement(cfm, restrictions);
            return new ParsedStatement.Prepared(stmt);
        }
    }
}
