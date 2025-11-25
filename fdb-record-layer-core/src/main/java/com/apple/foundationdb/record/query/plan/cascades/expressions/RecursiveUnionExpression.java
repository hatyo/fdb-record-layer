/*
 * RecursiveUnionExpression.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This is a logical representation of a recursive union, a recursive union is similar to a normal unordered union, however
 * its legs have special execution semantics; just like a union, it returns the results verbatim of one particular
 * leg called the "initial state" leg. This leg provides the results required to seed the recursion happening during
 * the execution of the other leg, the "recursive state" leg. The recursive unions repeatedly executes the recursive
 * leg until it does not produce any more results (fix-point).
 */
public class RecursiveUnionExpression extends AbstractRelationalExpressionWithChildren {

    @Nonnull
    private final Quantifier initialStateQuantifier;

    @Nonnull
    private final Quantifier recursiveStateQuantifier;

    @Nonnull
    private final CorrelationIdentifier tempTableScanAlias;

    @Nonnull
    private final CorrelationIdentifier tempTableInsertAlias;

    @Nonnull
    private final TraversalStrategy traversalStrategy;

    @Nonnull
    private final Value resultValue;

    /**
     * Defines the traversal strategy for recursive union operations in Common Table Expressions (CTEs).
     * This enum specifies how the recursive leg of a recursive union should traverse and process
     * the intermediate results during query execution.
     */
    public enum TraversalOrder {
        /**
         * No specific traversal order is enforced. The implementation is free to choose
         * any traversal strategy that is most efficient, potentially mixing different
         * approaches or processing results as they become available.
         */
        ANY,

        /**
         * Depth-First Search (DFS) pre-order traversal. In this strategy, each node is
         * processed before its children, ensuring that parent records are handled before
         * their descendants in the recursive hierarchy. This is useful for scenarios
         * where you need to process parent records before processing their children.
         */
        PREORDER,

        /**
         * Level-order (Breadth-First Search/BFS) traversal. In this strategy, all nodes
         * at the current depth level are processed before moving to the next level.
         * This ensures that all records at depth N are processed before any records
         * at depth N+1, which is useful for scenarios requiring level-by-level processing.
         */
        LEVEL,

        /**
         * Depth-First Search (DFS) post-order traversal. In this strategy, each node is
         * processed after its children, ensuring that descendant records are handled before
         * their parents in the recursive hierarchy. This is useful for scenarios where you
         * need to process children before processing their parent records.
         */
        POSTORDER
    }

    public static final class TraversalStrategy {

        @Nonnull
        private final TraversalOrder traversalOrder;

        @Nonnull
        private final TraversalBehavior traversalBehavior;

        private TraversalStrategy(@Nonnull final TraversalOrder traversalOrder,
                                  @Nonnull final TraversalBehavior traversalBehavior) {
            this.traversalOrder = traversalOrder;
            this.traversalBehavior = traversalBehavior;
        }

        @Nonnull
        public TraversalBehavior getTraversalBehavior() {
            return traversalBehavior;
        }

        @Nonnull
        @VisibleForTesting
        public TraversalOrder getTraversalOrder() {
            return traversalOrder;
        }

        @Nonnull
        TraversalStrategy translate(@Nonnull final TranslationMap translationMap) {
            if (traversalBehavior == TraversalBehavior.DEFAULT_BEHAVIOR) {
                return this;
            }
            final var translatedDfsTraversalBehavior = traversalBehavior.translate(translationMap);
            if (translatedDfsTraversalBehavior == traversalBehavior) {
                return this;
            }
            return new TraversalStrategy(traversalOrder, translatedDfsTraversalBehavior);
        }

        public boolean preOrderTraversalAllowed() {
            return traversalOrder == TraversalOrder.ANY || traversalOrder == TraversalOrder.PREORDER;
        }

        public boolean postOrderTraversalAllowed() {
            return traversalOrder == TraversalOrder.ANY || traversalOrder == TraversalOrder.POSTORDER;
        }

        public boolean levelTraversalAllowed() {
            return traversalOrder == TraversalOrder.ANY || traversalOrder == TraversalOrder.LEVEL;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final TraversalStrategy that = (TraversalStrategy)o;
            return traversalOrder == that.traversalOrder && Objects.equals(traversalBehavior, that.traversalBehavior);
        }

        @Override
        public int hashCode() {
            return Objects.hash(traversalOrder, traversalBehavior);
        }

        @Nonnull
        public static TraversalStrategy ofPreOrderWithCheck(@Nullable final Value checkValue, boolean errorOnMismatch) {
            TraversalBehavior traversalBehavior = TraversalBehavior.noCheck();
            if (checkValue != null) {
                traversalBehavior = TraversalBehavior.check(checkValue, errorOnMismatch);
            }
            return new TraversalStrategy(TraversalOrder.PREORDER, traversalBehavior);
        }

        @Nonnull
        public static TraversalStrategy ofPreOrder() {
            return new TraversalStrategy(TraversalOrder.PREORDER, TraversalBehavior.noCheck());
        }

        @Nonnull
        public static TraversalStrategy ofPostOrderWithCheck(@Nullable final Value checkValue, boolean errorOnMismatch) {
            TraversalBehavior traversalBehavior = TraversalBehavior.noCheck();
            if (checkValue != null) {
                traversalBehavior = TraversalBehavior.check(checkValue, errorOnMismatch);
            }
            return new TraversalStrategy(TraversalOrder.POSTORDER, traversalBehavior);
        }

        @Nonnull
        public static TraversalStrategy ofPostOrder() {
            return new TraversalStrategy(TraversalOrder.PREORDER, TraversalBehavior.noCheck());
        }

        @Nonnull
        public static TraversalStrategy ofLevelOrder() {
            return new TraversalStrategy(TraversalOrder.PREORDER, TraversalBehavior.noCheck());
        }

        @Nonnull
        public static TraversalStrategy ofAnyOrder() {
            return new TraversalStrategy(TraversalOrder.ANY, TraversalBehavior.noCheck());
        }

        @Nonnull
        public static TraversalStrategy of(@Nonnull TraversalOrder traversalOrder, @Nonnull TraversalBehavior traversalBehavior ) {
            return new TraversalStrategy(traversalOrder, traversalBehavior);
        }

        public static final class TraversalBehavior {

            @Nonnull
            private static final TraversalBehavior DEFAULT_BEHAVIOR = new TraversalBehavior(null, false);

            @Nullable
            private final Value checkValue;

            private final boolean errorOnMismatch;

            private TraversalBehavior(@Nullable final Value checkValue,
                                     final boolean errorOnMismatch) {
                this.checkValue = checkValue;
                this.errorOnMismatch = errorOnMismatch;
            }

            @Nonnull
            public TraversalBehavior translate(@Nonnull TranslationMap translationMap) {
                if (checkValue == null) {
                    return this;
                }
                final var translatedCheckValue = checkValue.translateCorrelations(translationMap);
                if (translatedCheckValue == checkValue) {
                    return this;
                }
                return new TraversalBehavior(translatedCheckValue, errorOnMismatch);
            }

            @Override
            public boolean equals(final Object o) {
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final TraversalBehavior that = (TraversalBehavior)o;
                return errorOnMismatch == that.errorOnMismatch && Objects.equals(checkValue, that.checkValue);
            }

            @Override
            public int hashCode() {
                return Objects.hash(checkValue, errorOnMismatch);
            }

            public boolean isNoCheck() {
                return this == DEFAULT_BEHAVIOR;
            }

            @Nonnull
            public Value getCheckFunction() {
                Verify.verify(checkValue != null);
                return checkValue;
            }

            public boolean isErrorOnMismatch() {
                return errorOnMismatch;
            }

            @Nonnull
            public static TraversalBehavior noCheck() {
                return DEFAULT_BEHAVIOR;
            }

            @Nonnull
            public static TraversalBehavior check(@Nonnull final Value checkFunction, final boolean errorOnMismatch) {
                return new TraversalBehavior(checkFunction, errorOnMismatch);
            }
        }
    }

    public RecursiveUnionExpression(@Nonnull final Quantifier initialState,
                                    @Nonnull final Quantifier recursiveState,
                                    @Nonnull final CorrelationIdentifier tempTableScanAlias,
                                    @Nonnull final CorrelationIdentifier tempTableInsertAlias,
                                    @Nonnull final TraversalStrategy traversalStrategy) {
        this.initialStateQuantifier = initialState;
        this.recursiveStateQuantifier = recursiveState;
        this.tempTableScanAlias = tempTableScanAlias;
        this.tempTableInsertAlias = tempTableInsertAlias;
        this.traversalStrategy = traversalStrategy;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier));
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        Streams.concat(initialStateQuantifier.getCorrelatedTo().stream(),
                        recursiveStateQuantifier.getCorrelatedTo().stream())
                // filter out the correlations that are satisfied by this plan
                .filter(alias -> !alias.equals(tempTableInsertAlias) && !alias.equals(tempTableScanAlias))
                .forEach(builder::add);
        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    public TraversalStrategy getTraversalStrategy() {
        return traversalStrategy;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(initialStateQuantifier, recursiveStateQuantifier);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecursiveUnionExpression)) {
            return false;
        }
        final var otherRecursiveUnionExpression = (RecursiveUnionExpression)otherExpression;
        return traversalStrategy.equals(otherRecursiveUnionExpression.traversalStrategy) &&
                (tempTableScanAlias.equals(otherRecursiveUnionExpression.tempTableScanAlias)
                        || equivalences.containsMapping(tempTableScanAlias, otherRecursiveUnionExpression.tempTableScanAlias)) &&
                (tempTableInsertAlias.equals(otherRecursiveUnionExpression.tempTableInsertAlias)
                         || equivalences.containsMapping(tempTableInsertAlias, otherRecursiveUnionExpression.tempTableInsertAlias));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getTempTableScanAlias(), getTempTableInsertAlias(), traversalStrategy);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // intentional
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(tempTableScanAlias)
                && !translationMap.containsSourceAlias(tempTableInsertAlias));
        final var translatedInitialStateQun = translatedQuantifiers.get(0);
        final var translatedRecursiveStateQun = translatedQuantifiers.get(1);
        final var translatedTraversalStrategy = traversalStrategy.translate(translationMap);
        return new RecursiveUnionExpression(translatedInitialStateQun, translatedRecursiveStateQun,
                tempTableScanAlias, tempTableInsertAlias, translatedTraversalStrategy);
    }

    @Nonnull
    public CorrelationIdentifier getTempTableScanAlias() {
        return tempTableScanAlias;
    }

    @Nonnull
    public CorrelationIdentifier getTempTableInsertAlias() {
        return tempTableInsertAlias;
    }

    @Nonnull
    public Quantifier getInitialStateQuantifier() {
        return initialStateQuantifier;
    }

    @Nonnull
    public Quantifier getRecursiveStateQuantifier() {
        return recursiveStateQuantifier;
    }

    public boolean preOrderTraversalAllowed() {
        return traversalStrategy.preOrderTraversalAllowed();
    }

    public boolean postOrderTraversalAllowed() {
        return traversalStrategy.postOrderTraversalAllowed();
    }

    public boolean dfsTraversalAllowed() {
        return preOrderTraversalAllowed() || postOrderTraversalAllowed();
    }

    public boolean levelTraversalAllowed() {
        return traversalStrategy.levelTraversalAllowed();
    }
}
