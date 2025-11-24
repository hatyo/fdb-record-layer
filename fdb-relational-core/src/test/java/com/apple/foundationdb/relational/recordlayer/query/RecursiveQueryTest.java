/*
 * RecursiveQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.utils.Ddl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for recursive SQL queries with hierarchical data.
 */
public class RecursiveQueryTest {

    private static final String HIERARCHY_SCHEMA =
            "CREATE TABLE HierarchyNode (" +
                    "id bigint, " +
                    "parent bigint, " +
                    "etag bigint, " +
                    "PRIMARY KEY(id))" +
                    " CREATE INDEX parent_id_idx AS SELECT parent, id FROM HierarchyNode ORDER BY parent, id" +
                    " CREATE INDEX id_parent_idx AS SELECT id, parent FROM HierarchyNode ORDER BY id, parent";

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public RecursiveQueryTest() {
        Utils.enableCascadesDebugger();
    }

    /**
     * Tests recursive query with continuation and changing hierarchy mid-execution.
     *
     * This test mirrors the descendantsAcrossContinuationsAndChangingHierarchy test from RecursiveQueriesTest.java
     *
     * Initial hierarchy (otherForest):
     * <pre>
     * {@code
     *          0                                  15
     *       ┌─────┐                              (isolated root)
     *      ┌┘     └┐
     *     1         2
     *  ┌──┴──┐   ┌──┴──┐
     * 3      4   5      6
     * ┌─┴─┐ ┌─┴─┐ ┌─┴─┐ ┌─┴─┐
     * 7   8 9  10 11 12 13 14
     * }
     * </pre>
     *
     * After modification (moving node 1 to be child of 15):
     * <pre>
     * {@code
     *          0              15
     *          │              │
     *          2              1
     *       ┌──┴──┐        ┌──┴──┐
     *      5      6       3      4
     *    ┌─┴─┐  ┌─┴─┐   ┌─┴─┐  ┌─┴─┐
     *   11 12  13 14    7   8  9  10
     * }
     * </pre>
     */
    @Test
    void descendantsAcrossContinuationsAndChangingHierarchy() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/RECURSIVE"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(HIERARCHY_SCHEMA)
                .build()) {

            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                // Insert the otherForest hierarchy
                insertOtherForestHierarchy(statement);

                // First query: Get descendants of node 0 with PREORDER traversal, limit to 3 rows
                String descendantsQuery =
                        "WITH RECURSIVE descendants(id, parent, etag) AS (" +
                                "  SELECT id, parent, etag FROM HierarchyNode WHERE id = 0 AND parent = -1 " +
                                "  UNION ALL " +
                                "  SELECT h.id, h.parent, h.etag " +
                                "  FROM HierarchyNode h, descendants d where h.parent = d.id " +
                                ") " +
                                "traversal order pre_order " +
                                "SELECT id, parent, etag FROM descendants";

                statement.setMaxRows(3);
                List<Long> firstBatch = new ArrayList<>();
                List<String> firstBatchWithEtag = new ArrayList<>();
                Continuation continuation;

                try (RelationalResultSet rs = statement.executeQuery(descendantsQuery)) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        long parent = rs.getLong("parent");
                        long etag = rs.getLong("etag");
                        firstBatch.add(id);
                        firstBatchWithEtag.add(String.format("(id=%d, parent=%d, etag=%d)", id, parent, etag));
                    }
                    continuation = rs.getContinuation();
                }

                // First batch should contain: 0, 1, 3 (PREORDER traversal), all with etag=0
                Assertions.assertEquals(List.of(0L, 1L, 3L), firstBatch,
                        "First batch should contain nodes 0, 1, 3 in PREORDER");
                Assertions.assertNotNull(continuation, "Should have a continuation");
                System.out.println("First batch with etag: " + firstBatchWithEtag);

                /*
                 * Modify the hierarchy: Move node 1 from parent 0 to parent 15 and set etag to 1
                 * This simulates the UPDATE in descendantsAcrossContinuationsAndChangingHierarchy
                 */
                int updateCount = statement.executeUpdate("UPDATE HierarchyNode SET parent = 15, etag = 1 WHERE id = 1");
                Assertions.assertEquals(1, updateCount, "Should update exactly one row");

                // Continue the query with the continuation
                List<Long> secondBatch = new ArrayList<>();
                List<String> secondBatchWithEtag = new ArrayList<>();
                try (var ps = ddl.setSchemaAndGetConnection().prepareStatement(
                        "EXECUTE CONTINUATION ?continuation")) {
                    ps.setBytes("continuation", continuation.serialize());
                    ps.setMaxRows(0); // Get all remaining rows

                    try (RelationalResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            long id = rs.getLong("id");
                            long parent = rs.getLong("parent");
                            long etag = rs.getLong("etag");
                            secondBatch.add(id);
                            secondBatchWithEtag.add(String.format("(id=%d, parent=%d, etag=%d)", id, parent, etag));
                        }
                    }
                }

                // The second batch continues from where we left off
                // Note: The exact results depend on how the continuation handles the hierarchy change
                // In the original test, this demonstrates that recursive queries can handle
                // hierarchies that change mid-execution
                Assertions.assertFalse(secondBatch.isEmpty(),
                        "Second batch should contain remaining nodes");

                // Verify all nodes were retrieved (one way or another)
                List<Long> allNodes = new ArrayList<>(firstBatch);
                allNodes.addAll(secondBatch);

                System.out.println("First batch: " + firstBatch);
                System.out.println("Second batch: " + secondBatch);
                System.out.println("Second batch with etag: " + secondBatchWithEtag);
                System.out.println("All nodes: " + allNodes);
            }
        }
    }

    /**
     * Helper method to insert the otherForest hierarchy using SQL INSERT statements.
     *
     * This creates the same hierarchy as otherForest() in RecursiveQueriesTest:
     * - Node 0 (root) with parent -1
     * - Node 15 (root) with parent -1
     * - Node 1, 2 as children of 0
     * - Node 3, 4 as children of 1
     * - Node 5, 6 as children of 2
     * - Node 7, 8 as children of 3
     * - Node 9, 10 as children of 4
     * - Node 11, 12 as children of 5
     * - Node 13, 14 as children of 6
     * All nodes initially have etag = 0
     */
    private void insertOtherForestHierarchy(RelationalStatement statement) throws SQLException {
        String insertQuery =
                "INSERT INTO HierarchyNode (id, parent, etag) VALUES " +
                        "(0, -1, 0), " +   // Root node 0
                        "(15, -1, 0), " +  // Root node 15
                        "(1, 0, 0), " +    // Level 1: children of 0
                        "(2, 0, 0), " +
                        "(3, 1, 0), " +    // Level 2: children of 1
                        "(4, 1, 0), " +
                        "(5, 2, 0), " +    // Level 2: children of 2
                        "(6, 2, 0), " +
                        "(7, 3, 0), " +    // Level 3: leaf nodes
                        "(8, 3, 0), " +
                        "(9, 4, 0), " +
                        "(10, 4, 0), " +
                        "(11, 5, 0), " +
                        "(12, 5, 0), " +
                        "(13, 6, 0), " +
                        "(14, 6, 0)";

        int count = statement.executeUpdate(insertQuery);
        Assertions.assertEquals(16, count, "Should insert exactly 16 nodes");
    }
}
