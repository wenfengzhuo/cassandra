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

import org.apache.cassandra.auth.Permission;
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
import org.apache.cassandra.db.ReadExecutionController;
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
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 *
 * @author Wenfeng Zhuo (wz2366@columbia.edu)
 * @createAt 01-18-2017
 */
public class SelectTimestampStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(SelectTimestampStatement.class);

    private static final ColumnIdentifier TIMESTAMP_COLUMN = new ColumnIdentifier("timestamp", false);

    public CFMetaData cfm;

    public int boundTerms;

    public StatementRestrictions restrictions;

    private List<ColumnSpecification> columns;

    public SelectTimestampStatement(CFMetaData cfm, int boundTerms, StatementRestrictions restrictions) {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.restrictions = restrictions;
        this.columns = Arrays.asList(new ColumnSpecification(cfm.ksName,
                                                             cfm.cfName,
                                                             TIMESTAMP_COLUMN,
                                                             LongType.instance));
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    /**
     * The same logic with {@link SelectStatement}
     *
     * @param state the current client state
     * @throws UnauthorizedException
     * @throws InvalidRequestException
     */
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (cfm.isView())
        {
            CFMetaData baseTable = View.findBaseTable(cfm.ksName, cfm.cfName);
            if (baseTable != null)
                state.hasColumnFamilyAccess(baseTable, Permission.SELECT);
        }
        else
        {
            state.hasColumnFamilyAccess(cfm, Permission.SELECT);
        }

        for (Function function : getFunctions())
            state.ensureHasPermission(Permission.EXECUTE, function);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // No-op. Validations are done in RawStatement.prepare()
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        ReadQuery readQuery = getReadQuery(options);
        return retrieveResult(readQuery, options, state, queryStartNanoTime);
    }

    public ResultMessage executeInternal(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        ReadQuery readQuery = getReadQuery(options);
        return retrieveResultInternal(readQuery);
    }

    /**
     * Get ReadQuery from restrictions. The ReadQuery will created as a SinglePartitionReadCommand.
     * @param options
     * @return
     */
    private ReadQuery getReadQuery(QueryOptions options) {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);
        List<SinglePartitionReadCommand> commands = new ArrayList<>();

        for (ByteBuffer key : keys) {

            QueryProcessor.validateKey(key);
            DecoratedKey decoratedKey = cfm.decorateKey(ByteBufferUtil.clone(key));

            ClusteringIndexFilter clusteringIndexFilter = new ClusteringIndexNamesFilter(restrictions.getClusteringColumns(options), false);

            // No columns will be selected since we only care the livenessinfo of the primary key of rows
            ColumnFilter columnFilter = ColumnFilter.selectionBuilder().build();

            commands.add(SinglePartitionReadCommand.create(cfm,
                                                           FBUtilities.nowInSeconds(),
                                                           decoratedKey,
                                                           ColumnFilter.selectionBuilder().build(),
                                                           clusteringIndexFilter));
        }
        ReadQuery readQuery = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);
        return readQuery;
    }

    /**
     * Retrieve results from a {@link ReadQuery}. This is used by users.
     * @param readQuery
     * @param options
     * @param state
     * @param queryStartNanoTime
     * @return
     */
    private ResultMessage retrieveResult(ReadQuery readQuery, QueryOptions options, QueryState state, long queryStartNanoTime) {
        try (PartitionIterator data = readQuery.execute(options.getConsistency(), state.getClientState(), queryStartNanoTime)) {
            return processResult(data);
        }
    }

    /**
     * Retrieve result from a {@link ReadQuery}. This is used internally.
     * @param readQuery
     * @return
     */
    private ResultMessage retrieveResultInternal(ReadQuery readQuery) {
        try (ReadExecutionController controller = readQuery.executionController())
        {
            try (PartitionIterator data = readQuery.executeInternal(controller))
            {
                return processResult(data);
            }
        }
    }

    /**
     * Process the data, and build the result message to be returned.
     * @param data
     * @return
     */
    private ResultMessage processResult(PartitionIterator data) {
        long maxTimestamp = Long.MIN_VALUE;
        boolean hasRow = false;

        while (data.hasNext()) {
            RowIterator rows = data.next();
            while (rows.hasNext()) {
                hasRow = true;
                Row row = rows.next();
                maxTimestamp = Math.max(maxTimestamp, row.primaryKeyLivenessInfo().timestamp());
            }
        }

        ResultSet resultSet = new ResultSet(columns);
        if (hasRow)
        {
            // If there are some rows under this partition key, then we add the max timestamp
            // to result set. Or else, we should not return anything.
            resultSet.addRow(Arrays.asList(ByteBufferUtil.bytes(maxTimestamp)));
        }

        return new ResultMessage.Rows(resultSet);
    }

    public Iterable<Function> getFunctions()
    {
        List<Function> functions = new ArrayList<>();
        restrictions.addFunctionsTo(functions);
        return functions;
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
            checkTrue(partitionKeys.size() == values.size(), "The number of values for partition keys does not match the table definition");

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

            SelectTimestampStatement stmt = new SelectTimestampStatement(cfm, boundNames.size(), restrictions);
            return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(cfm));
        }
    }

}
