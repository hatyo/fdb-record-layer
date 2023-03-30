/*
 * QueryPredicateDemoTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This is a demo test of {@link QueryPredicate}.
 */
public class QueryPredicateDemoTest extends FDBRecordStoreQueryTestBase {

    private static boolean isPrime(int number) {
        if (number <= 1) {
            return false;
        }
        for (int i = 2; i <= number / 2; i++) {
            if ((number % i) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Example {@link QueryPredicate} that returns {@code True} if a number is prime, otherwise {@code False}.
     */
    static class PrimaryNumberPredicate implements QueryPredicate {

        @Nonnull
        private final Value value;

        public PrimaryNumberPredicate() {
            final var type = Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor());
            value = FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("myAlias"), type), "num_value_unique");
        }

        @Override
        public int planHash(@Nonnull final PlanHashKind hashKind) {
            return 42;
        }

        @Override
        public int semanticHashCode() {
            return 42;
        }

        @Nonnull
        @Override
        public Iterable<? extends QueryPredicate> getChildren() {
            return ImmutableList.of();
        }

        @Nonnull
        @Override
        public QueryPredicate withChildren(final Iterable<? extends QueryPredicate> newChildren) {
            return new PrimaryNumberPredicate();
        }

        @Nullable
        @Override
        public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
            int number = (Integer)Objects.requireNonNull(value.eval(store, context));
            return isPrime(number);
        }

        private static boolean isPrime(int number) {
            if (number <= 1) {
                return false;
            }
            for (int i = 2; i <= number / 2; i++) {
                if ((number % i) == 0) {
                    return false;
                }
            }
            return true;
        }
    }

    @Test
    void printPrimaryNumbers() throws Exception {
        addRecords();

        final var context = EvaluationContext.empty();
        final var predicate = new PrimaryNumberPredicate();

        try (FDBRecordContext recordContext = openContext()) {
            openSimpleRecordStore(recordContext);
            try (var cursor = recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, null, ScanProperties.FORWARD_SCAN)
                    .map(m -> QueryResult.fromQueriedRecord(recordStore.queriedRecord(m)))
                    .filter(m -> {
                        final var nestedContext = context.withBinding(Bindings.Internal.CORRELATION.bindingName("myAlias"), m);
                        return predicate.eval(null, nestedContext);
                    })
                    .map(m -> Objects.requireNonNull((DynamicMessage)m.getDatum()).getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByName("num_value_unique"))))
            {
                final var list = cursor.asStream().collect(Collectors.toList());
                Assertions.assertTrue(list.stream().allMatch(i -> QueryPredicateDemoTest.isPrime((Integer)i)));
            }
        }
    }

    /**
     * Adds a number of records ranging between [0, 100[ for field {@code num_value_unique}.
     * @throws Exception if opening the record store fails.
     */
    protected void addRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(i);
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(i % 5);
                for (int j = 0; j < i % 10; j++) {
                    recBuilder.addRepeater(j);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }
}
