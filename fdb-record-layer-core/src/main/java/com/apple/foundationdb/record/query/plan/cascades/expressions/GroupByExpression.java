/*
 * GroupByExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {

    @Nullable
    private final FieldValue groupingValue;

    @Nonnull
    private final AggregateValue aggregateValue;

    @Nonnull
    private final Supplier<Value> computeResultSupplier;

    @Nonnull
    private final Supplier<RequestedOrdering> computeRequestedOrderingSupplier;

    @Nonnull
    private final Quantifier inner;

    /**
     * Creates a new instance of {@link GroupByExpression}.
     *
     * @param aggregateValue The aggregation {@code Value} applied to each group.
     * @param groupingValue The grouping {@code Value} used to determine individual groups, can be {@code null} indicating no grouping.
     * @param inner The underlying source of tuples to be grouped.
     */
    public GroupByExpression(@Nonnull final AggregateValue aggregateValue,
                             @Nullable final FieldValue groupingValue,
                             @Nonnull final Quantifier inner) {
        this.groupingValue = groupingValue;
        this.aggregateValue = aggregateValue;
        this.computeResultSupplier = Suppliers.memoize(this::computeResultValue);
        this.computeRequestedOrderingSupplier = Suppliers.memoize(this::computeRequestOrdering);
        this.inner = inner;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return getResultValue().getCorrelatedTo();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return computeResultSupplier.get();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        final var otherGroupByExpr = ((GroupByExpression)other);

        if ( (otherGroupByExpr.getGroupingValue() == null) ^ (getGroupingValue() == null) ) {
            return false;
        }

        if (otherGroupByExpr.getGroupingValue() != null) {
            return Objects.requireNonNull(getGroupingValue()).semanticEquals(otherGroupByExpr.getGroupingValue(), equivalences)
                   && getAggregateValue().semanticEquals(otherGroupByExpr.getAggregateValue(), equivalences);
        } else {
            return getAggregateValue().semanticEquals(otherGroupByExpr.getAggregateValue(), equivalences);
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final AggregateValue translatedAggregateValue = getAggregateValue().translateCorrelations(translationMap);
        final Value translatedGroupingValue = getGroupingValue() == null ? null : getGroupingValue().translateCorrelations(translationMap);
        Verify.verify(translatedGroupingValue instanceof FieldValue);
        if (translatedAggregateValue != getAggregateValue() || translatedGroupingValue != getGroupingValue()) {
            return new GroupByExpression(translatedAggregateValue, (FieldValue)translatedGroupingValue, Iterables.getOnlyElement(translatedQuantifiers));
        }
        return this;
    }

    @Override
    public String toString() {
        if (getGroupingValue() != null) {
            return "GroupBy(" + getGroupingValue() + "), aggregationValue: " + getAggregateValue() + ", resultValue: " + computeResultSupplier.get();
        } else {
            return "GroupBy(NULL), aggregationValue: " + getAggregateValue() + ", resultValue: " + computeResultSupplier.get();
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        if (getGroupingValue() == null) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValue().toString()))),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}", "GROUP BY {{grouping}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValue().toString()),
                                    "grouping", Attribute.gml(getGroupingValue().toString()))),
                    childGraphs);
        }
    }

    @Nullable
    public FieldValue getGroupingValue() {
        return groupingValue;
    }

    @Nonnull
    public AggregateValue getAggregateValue() {
        return aggregateValue;
    }

    /**
     * Returns the ordering requirements of the underlying scan for the group by to work. This is used by the planner
     * to choose a compatibly-ordered access path.
     *
     * @return The ordering requirements.
     */
    @Nonnull
    public RequestedOrdering getRequestedOrdering() {
        return computeRequestedOrderingSupplier.get();
    }

    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var quantifier = Iterables.getOnlyElement(getQuantifiers());

        // if the match requires, for the moment, any, compensation, we reject it.
        final Optional<Compensation> childCompensation = matchInfo.getChildPartialMatch(quantifier)
                                .map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap));

        return childCompensation.orElse(Compensation.noCompensation());

//        // probably not important because we can never get unless we have done the check already.
//        if (childCompensation.isPresent() && (childCompensation.get().isImpossible() || childCompensation.get().isNeeded())) {
//            // check if the remaining computation is something we can work with.
//            final var queryExpression = partialMatch.getQueryExpression();
//            for (final var candidateExpression : partialMatch.getCandidateRef().getMembers()) {
//                if (!expressionAreGroupCompatible(queryExpression, candidateExpression, partialMatch.getBoundAliasMap())) {
//                    return Compensation.impossibleCompensation();
//                }
//            }
//        }

        //return Compensation.noCompensation();
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {

        // the candidate must be a GROUP-BY expression.
        if (candidateExpression.getClass() != this.getClass()) {
            return ImmutableList.of();
        }

        final var otherGroupByExpression = (GroupByExpression)candidateExpression;

        // the grouping values are encoded directly in the underlying SELECT-WHERE, reaching this point means that the
        // grouping values had exact match, so we don't need to check them.


        // check that aggregate value is the same.
        final var otherAggregateValue = otherGroupByExpression.getAggregateValue();
        if (aggregateValue.subsumedBy(otherAggregateValue, aliasMap)) {
            // placeholder for information needed for later compensation.
            return tryMerge(partialMatchMap, aliasMap)
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }
        return ImmutableList.of();
    }

    @Nonnull
    private Optional<MatchInfo> tryMerge(@Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap, @Nonnull final AliasMap aliasMap) {
        final var parameterMapsBuilder = ImmutableList.<Map<CorrelationIdentifier, ComparisonRange>>builder();
        final var matchInfos = PartialMatch.matchesFromMap(partialMatchMap);

        matchInfos.forEach(matchInfo -> parameterMapsBuilder.add(matchInfo.getParameterBindingMap()));

        final var regularQuantifiers = partialMatchMap.keySet()
                .stream()
                .map(Equivalence.Wrapper::get)
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach || quantifier instanceof Quantifier.Physical)
                .collect(Collectors.toCollection(Sets::newIdentityHashSet));

        final List<MatchedOrderingPart> orderingParts;
        if (regularQuantifiers.size() == 1) {
            final var regularQuantifier = Iterables.getOnlyElement(regularQuantifiers);
            final var partialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(regularQuantifier));
            orderingParts = partialMatch.getMatchInfo().getMatchedOrderingParts();
        } else {
            orderingParts = ImmutableList.of();
        }

        final Optional<Map<CorrelationIdentifier, ComparisonRange>> mergedParameterBindingsOptional =
                MatchInfo.tryMergeParameterBindings(parameterMapsBuilder.build());

        for (final var regularQuantifier : regularQuantifiers) {
            final var partialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(regularQuantifier));
            final var remainingComputationValueMaybe = partialMatch.getMatchInfo().getRemainingComputationValueOptional();
            if (remainingComputationValueMaybe.isEmpty()) {
                continue;
            }
            // check if the remaining computation is something we can work with.
            final var queryExpression = partialMatch.getQueryExpression();
            final var candidateExpression = Iterables.getOnlyElement(partialMatch.getCandidateRef().getMembers());
            if (!expressionsAreGroupCompatible(queryExpression, candidateExpression, partialMatch.getBindingPredicates(), partialMatch.getMatchInfo().getAliasMap())) {
                return Optional.empty(); // not match.
            }
        }

        // consolidate all grouping sets mappings.

        return mergedParameterBindingsOptional
                .map(mergedParameterBindings -> new MatchInfo(mergedParameterBindings,
                        partialMatchMap,
                        PredicateMap.empty(),
                        orderingParts,
                        Optional.empty(),
                        aliasMap));
    }

    private boolean expressionsAreGroupCompatible(@Nonnull final RelationalExpression queryExpression,
                                                  @Nonnull final RelationalExpression candidateExpression,
                                                  @Nonnull final Set<QueryPredicate> boundPredicates,
                                                  @Nonnull final AliasMap aliasMap) {
        if (!(queryExpression instanceof SelectExpression)) {
            return false;
        }
        if (!(candidateExpression instanceof SelectExpression)) {
            return false;
        }
        final var querySelectExpression = (SelectExpression)queryExpression;
        final var candidateSelectExpression = (SelectExpression)candidateExpression;
        final var queryResultValue = Values.deconstructRecord(querySelectExpression.getResultValue());
        final var queryGroupedValues = queryResultValue.isEmpty() ? ImmutableList.<Value>of() : Values.deconstructRecord(queryResultValue.get(0));
        final var candidateResultValue = Values.deconstructRecord(candidateSelectExpression.getResultValue());
        final var candidateGroupedValues = candidateResultValue.isEmpty() ? ImmutableList.<Value>of() : Values.deconstructRecord(candidateResultValue.get(0));
        // wrong, get it from child match predicate map.
        // if the child match needs a compensation.

        final var queryEqualityPredicatedValues = boundPredicates
                .stream()
                .filter(p -> p instanceof PredicateWithValue)
                .map(p -> (PredicateWithValue)p)
                .filter(PredicateWithValue::isEquality)
                .map(PredicateWithValue::getValue)
                .collect(Collectors.toSet());

        // a, b (where c)
        // a, b, c
        int offset = 0;
        ImmutableList.Builder<Value> remappedValueBuilder = ImmutableList.builder();
        final var matchedCandidateGroups = new BitSet(candidateGroupedValues.size());
        for (final var queryGroupedValue : queryGroupedValues) {
            int relativeOffset = 0;
            boolean found = false;
            for (final var candidateGroupedValue : candidateGroupedValues) {
                if (queryGroupedValue.semanticEquals(candidateGroupedValue, aliasMap)) {
                    if (relativeOffset < offset) {
                        return false; // different order, bailout.
                    }
                    remappedValueBuilder.add(candidateGroupedValue);
                    found = true;
                    break;
                }
                relativeOffset++;
            }
            if (!found) {
                return false;
            } else {
                matchedCandidateGroups.set(relativeOffset);
            }
            offset++;
        }
        // now check whether the "holes" i.e. unmatched candidate grouped values can be attributed
        // to values on the query-side that are equality-predicated.
        for (int i = 0; i < candidateGroupedValues.size(); i++) {
            final int j = i;
            if (!matchedCandidateGroups.get(i) && queryEqualityPredicatedValues.stream().noneMatch(queryValue -> queryValue.semanticEquals(candidateGroupedValues.get(j), aliasMap))) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    private Value computeResultValue() {
        final var aggregateColumn = Column.unnamedOf(getAggregateValue());
        if (getGroupingValue() == null) {
            return RecordConstructorValue.ofColumns(ImmutableList.of(aggregateColumn));
        } else {
            final var groupingColumn = Column.unnamedOf(getGroupingValue());
            return RecordConstructorValue.ofColumns(ImmutableList.of(groupingColumn, aggregateColumn));
        }
    }

    @Nonnull
    private RequestedOrdering computeRequestOrdering() {
        if (groupingValue == null || groupingValue.isConstant()) {
            return RequestedOrdering.preserve();
        }

        final var groupingValueType = groupingValue.getResultType();
        Verify.verify(groupingValueType instanceof Type.Record);

        return new RequestedOrdering(
                ImmutableList.of(OrderingPart.of(groupingValue)),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS);
    }
}
