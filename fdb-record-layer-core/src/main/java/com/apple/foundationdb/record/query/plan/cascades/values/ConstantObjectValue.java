/*
 * ConstantObjectValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a constant value that correlates to a value of a constant binding.
 */
public class ConstantObjectValue implements QuantifiedValue {

    @Nonnull
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Constant-Object-Value");

    @Nonnull
    private final CorrelationIdentifier alias;

    private final int ordinal;

    @Nonnull
    private final Type resultType;

    private ConstantObjectValue(@Nonnull final CorrelationIdentifier alias, int ordinal, @Nonnull final Type resultType) {
        this.alias = alias;
        this.ordinal = ordinal;
        this.resultType = resultType;
    }

    @Override
    @Nonnull
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public Value rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return ConstantObjectValue.of(targetAlias, ordinal, resultType);
    }

    @Override
    @Nonnull
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    public Value replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return fieldValue;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return context.dereferenceConstant(alias, ordinal);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.planHash(hashKind, BASE_HASH);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return formatter.getQuantifierName(alias) + "@" + ordinal;
    }

    @Override
    public String toString() {
        return "@" + alias + "@@" + ordinal; // "@"-prefix for constant.
    }

    @Nonnull
    public static ConstantObjectValue of(@Nonnull final CorrelationIdentifier alias, int ordinal, @Nonnull final Type resultType) {
        return new ConstantObjectValue(alias, ordinal, resultType);
    }
}