/*
 * RecordQueryRecursivePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.RecursiveCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryRecursiveDfsJoinPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryRecursiveDfsJoinPlan.PDfsTraversalStrategy;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RecursiveUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that recursively applies a plan to earlier results, starting with a root plan.
 */
@API(API.Status.INTERNAL)
public class RecordQueryRecursiveDfsJoinPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Recursive-Plan");

    @Nonnull
    private final Quantifier.Physical rootQuantifier;
    @Nonnull
    private final Quantifier.Physical childQuantifier;
    @Nonnull
    private final CorrelationIdentifier priorValueCorrelation;
    @Nonnull
    private final Value resultValue;
    private final boolean isPreorder;
    @Nonnull
    private final RecursiveUnionExpression.TraversalStrategy.TraversalBehavior traversalBehavior;

    public RecordQueryRecursiveDfsJoinPlan(@Nonnull final Quantifier.Physical rootQuantifier,
                                           @Nonnull final Quantifier.Physical childQuantifier,
                                           @Nonnull final CorrelationIdentifier priorValueCorrelation,
                                           final boolean isPreorder,
                                           @Nonnull final RecursiveUnionExpression.TraversalStrategy.TraversalBehavior traversalBehavior) {
        this.rootQuantifier = rootQuantifier;
        this.childQuantifier = childQuantifier;
        this.priorValueCorrelation = priorValueCorrelation;
        this.isPreorder = isPreorder;
        this.traversalBehavior = traversalBehavior;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(rootQuantifier, childQuantifier));
    }

    @Nonnull
    public Quantifier.Physical getRootQuantifier() {
        return rootQuantifier;
    }

    @Nonnull
    public Quantifier.Physical getChildQuantifier() {
        return childQuantifier;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {

        final var nestedExecuteProperties = executeProperties.clearSkipAndLimit();
        return RecursiveCursor.create(
                        rootContinuation ->
                                rootQuantifier.getRangesOverPlan().executePlan(store, context, rootContinuation, nestedExecuteProperties),
                        (parentResult, depth, innerContinuation) -> {
                            // TODO: Consider binding depth as well.
                            final EvaluationContext childContext = context.withBinding(Bindings.Internal.CORRELATION.bindingName(priorValueCorrelation.getId()), parentResult);
                            return childQuantifier.getRangesOverPlan().executePlan(store, childContext, innerContinuation, nestedExecuteProperties);
                        },
                        traversalBehavior.isNoCheck() ? null :
                            queryResult -> {
                                final var childContext = context.withBinding(Bindings.Internal.CORRELATION.bindingName(childQuantifier.getAlias().getId()), queryResult);
                                final var value = traversalBehavior.getCheckFunction().eval(store, childContext);
                                return value == null ? null : PlanSerialization.valueObjectToProto(value).toByteArray();
                            },
                        continuation,
                        isPreorder,
                        traversalBehavior.isErrorOnMismatch()
                ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(RecursiveCursor.RecursiveValue::getValue);
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return ImmutableList.of(rootQuantifier.getRangesOverPlan(), childQuantifier.getRangesOverPlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        Streams.concat(rootQuantifier.getCorrelatedTo().stream(),
                        childQuantifier.getCorrelatedTo().stream())
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !alias.equals(priorValueCorrelation))
                .forEach(builder::add);
        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedTo();
    }

    @Override
    public boolean isReverse() {
        return Quantifiers.isReversed(Quantifiers.narrow(Quantifier.Physical.class, getQuantifiers()));
    }

    @Override
    public boolean isStrictlySorted() {
        return true;
    }

    public boolean isPreorder() {
        return isPreorder;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public RecursiveUnionExpression.TraversalStrategy.TraversalBehavior getTraversalBehavior() {
        return traversalBehavior;
    }

    @Nonnull
    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return semanticEqualsForResults(otherExpression, aliasMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean structuralEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        if (other == null || other.getClass() != RecordQueryRecursiveDfsJoinPlan.class) {
            return false;
        }

        final var otherExpression = (RecordQueryRecursiveDfsJoinPlan)other;

        for (final AliasMap boundCorrelatedReferencesMap : enumerateUnboundCorrelatedTo(equivalenceMap, otherExpression)) {
            final AliasMap.Builder boundCorrelatedToBuilder = boundCorrelatedReferencesMap.toBuilder();

            // assume prior aliases are equal.
            boundCorrelatedToBuilder.put(getPriorValueCorrelation(), otherExpression.getPriorValueCorrelation());

            // verify structural equality of the root quantifier.

            if (getRootQuantifier().structuralHashCode() != otherExpression.getRootQuantifier().structuralHashCode()) {
                continue;
            }
            if (!getRootQuantifier().structuralEquals(otherExpression.getRootQuantifier(), boundCorrelatedToBuilder.build())) {
                continue;
            }
            // root quantifiers are structurally equal, add their aliases to the alias map equivalence
            boundCorrelatedToBuilder.put(getRootQuantifier().getAlias(), otherExpression.getRootQuantifier().getAlias());

            // verify structural equality of the child quantifiers.
            if (getChildQuantifier().structuralHashCode() != otherExpression.getChildQuantifier().structuralHashCode()) {
                continue;
            }
            if (!getChildQuantifier().structuralEquals(otherExpression.getChildQuantifier(), boundCorrelatedToBuilder.build())) {
                continue;
            }
            // child quantifiers are also structurally equal, add their aliases to the alias map equivalence
            boundCorrelatedToBuilder.put(getChildQuantifier().getAlias(), otherExpression.getChildQuantifier().getAlias());

            // verify semantic equality of both expressions using the accumulated alias map equivalence
            if (equalsWithoutChildren(otherExpression, boundCorrelatedToBuilder.build())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue()); // priorValueCorrelation _is_ a child correlation due to the recursive nature of this plan.
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(priorValueCorrelation));
        return new RecordQueryRecursiveDfsJoinPlan(
                translatedQuantifiers.get(0).narrow(Quantifier.Physical.class),
                translatedQuantifiers.get(1).narrow(Quantifier.Physical.class),
                priorValueCorrelation,
                isPreorder,
                traversalBehavior
        );
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
    }

    @Override
    public int getComplexity() {
        return rootQuantifier.getRangesOverPlan().getComplexity() * childQuantifier.getRangesOverPlan().getComplexity();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren(), getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(rootQuantifier, childQuantifier);
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR,
                        ImmutableList.of("RECURSIVE {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(getResultValue().toString()))),
                childGraphs,
                getQuantifiers());
    }

    @Nonnull
    @Override
    public PRecordQueryRecursiveDfsJoinPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryRecursiveDfsJoinPlan.newBuilder()
                .setRootQuantifier(rootQuantifier.toProto(serializationContext))
                .setChildQuantifier(childQuantifier.toProto(serializationContext))
                .setPriorValueCorrelation(priorValueCorrelation.getId())
                .setTraversalBehavior(toProto(traversalBehavior, serializationContext))
                .build();
    }

    @Nonnull
    private static PRecordQueryRecursiveDfsJoinPlan.PTraversalBehavior toProto(@Nonnull final RecursiveUnionExpression.TraversalStrategy.TraversalBehavior traversalBehavior,
                                                                               @Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryRecursiveDfsJoinPlan.PTraversalBehavior.newBuilder();
        if (traversalBehavior.isNoCheck()) {
            return builder.build();
        }
        return builder.setErrorOnMismatch(traversalBehavior.isErrorOnMismatch())
                .setCheckValue(traversalBehavior.getCheckFunction().toValueProto(serializationContext)).build();
    }

    @Nonnull
    private static RecursiveUnionExpression.TraversalStrategy.TraversalBehavior fromProto(@Nonnull final PRecordQueryRecursiveDfsJoinPlan.PTraversalBehavior traversalBehavior,
                                                                                          @Nonnull final PlanSerializationContext serializationContext) {
        if (traversalBehavior.hasCheckValue()) {
            return RecursiveUnionExpression.TraversalStrategy.TraversalBehavior.check(Value.fromValueProto(serializationContext, traversalBehavior.getCheckValue()),
                    traversalBehavior.getErrorOnMismatch());
        } else {
            return RecursiveUnionExpression.TraversalStrategy.TraversalBehavior.noCheck();
        }
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecursiveDfsJoinPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    @SuppressWarnings("deprecation") // to maintain backward-compatibility
    public static RecordQueryRecursiveDfsJoinPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                            @Nonnull final PRecordQueryRecursiveDfsJoinPlan recordQueryRecursivePlanProto) {
        final boolean isPreorder = recordQueryRecursivePlanProto.getDfsTraversalStrategy().getNumber() == PDfsTraversalStrategy.PRE_ORDER_VALUE;
        return new RecordQueryRecursiveDfsJoinPlan(
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryRecursivePlanProto.getRootQuantifier())),
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryRecursivePlanProto.getChildQuantifier())),
                CorrelationIdentifier.of(Objects.requireNonNull(recordQueryRecursivePlanProto.getPriorValueCorrelation())),
                isPreorder,
                fromProto(recordQueryRecursivePlanProto.getTraversalBehavior(), serializationContext));
    }

    @Nonnull
    public CorrelationIdentifier getPriorValueCorrelation() {
        return priorValueCorrelation;
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryRecursiveDfsJoinPlan, RecordQueryRecursiveDfsJoinPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryRecursiveDfsJoinPlan> getProtoMessageClass() {
            return PRecordQueryRecursiveDfsJoinPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryRecursiveDfsJoinPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryRecursiveDfsJoinPlan recordQueryRecursivePlanProto) {
            return RecordQueryRecursiveDfsJoinPlan.fromProto(serializationContext, recordQueryRecursivePlanProto);
        }
    }
}
