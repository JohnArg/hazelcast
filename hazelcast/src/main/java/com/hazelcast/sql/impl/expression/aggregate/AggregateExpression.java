/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.exec.agg.AggregateExec;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Common parent for all aggregate accumulators.
 */
public abstract class AggregateExpression<T> implements DataSerializable {
    /** Distinct flag. */
    protected boolean distinct;

    /** Result type. */
    protected transient DataType resType;

    /** Parent executor. */
    protected transient AggregateExec parent;

    protected AggregateExpression() {
        // No-op.
    }

    protected AggregateExpression(boolean distinct) {
        this.distinct = distinct;
    }

    public void setup(AggregateExec parent) {
        this.parent = parent;
    }

    /**
     * Collect value of the aggregate.
     *
     * @param row Row.
     * @param collector Collector.
     */
    public abstract void collect(Row row, AggregateCollector collector);

    /**
     * Create new collector for the given expression.
     *
     * @param ctx Query context.
     * @return Collector.
     */
    public abstract AggregateCollector newCollector(QueryContext ctx);

    /**
     * @return Return type of the expression.
     */
    public DataType getType() {
        return DataType.notNullOrLate(resType);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(distinct);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        distinct = in.readBoolean();
    }
}