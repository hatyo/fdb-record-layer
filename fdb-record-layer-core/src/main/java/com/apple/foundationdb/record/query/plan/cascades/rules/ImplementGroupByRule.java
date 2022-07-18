/*
 * ImplementGroupByRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.AnyMatcher.any;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.groupByExpression;

/**
 * Rule for implementing logical {@code GROUP BY} into a physical streaming aggregate operator.
 */
public class ImplementGroupByRule extends PlannerRule<GroupByExpression> {

    @Nonnull
    private static final BindingMatcher<ExpressionRef<? extends RelationalExpression>> lowerRefMatcher = ReferenceMatchers.anyRef();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifierOverRef(lowerRefMatcher);
    @Nonnull
    private static final BindingMatcher<GroupByExpression> root =
            groupByExpression(any(innerQuantifierMatcher));

    public ImplementGroupByRule() {
        super(root, ImmutableSet.of(RequestedOrderingConstraint.REQUESTED_ORDERING));
    }

    @Override
    public void onMatch(@Nonnull final PlannerRuleCall call) {
        final var bindings = call.getBindings();

        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();
        final var groupByExpression = bindings.get(root);
        final var innerQuantifier = Iterables.getOnlyElement(groupByExpression.getQuantifiers());
        final var innerReference = innerQuantifier.getRangesOver();
        final var planPartitions = PlanPartition.rollUpTo(innerReference.getPlanPartitions(), OrderingProperty.ORDERING);

        for (final var planPartition  : planPartitions) {
            final var providedOrdering = planPartition.getAttributeValue(OrderingProperty.ORDERING);
            for (final RequestedOrdering requestedOrdering : requestedOrderings) {
                if (Ordering.satisfiesRequestedOrdering(providedOrdering, requestedOrdering)) {
                    GroupExpressionRef<RecordQueryPlan> newInnerPlanReference = GroupExpressionRef.from(planPartition.getPlans());
                    final var newPlanQuantifier = Quantifier.physical(newInnerPlanReference);
                    final var aliasMap = AliasMap.of(innerQuantifier.getAlias(), newPlanQuantifier.getAlias());
                    final var result = RecordQueryStreamingAggregationPlan.of(
                            newPlanQuantifier,
                            groupByExpression.getGroupingValue().rebase(aliasMap),
                            (AggregateValue)groupByExpression.getAggregateValue().rebase(aliasMap),
                            RecordQueryStreamingAggregationPlan::flattenedResults);
                    call.yield(GroupExpressionRef.of(result));
                }
            }
        }
    }
}