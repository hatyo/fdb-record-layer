/*
 * RecursiveQueryLargerHierarchyTest.java
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
 * Integration tests for recursive SQL queries with a larger hierarchical data structure.
 * This test uses a deeper hierarchy where nodes 0 and 15 share a common parent (20),
 * and node 20 has parent 21.
 */
public class RecursiveQueryLargerHierarchyTest {

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

    public RecursiveQueryLargerHierarchyTest() {
        Utils.enableCascadesDebugger();
    }

    /**
     * Tests recursive query with continuation and changing hierarchy mid-execution.
     *
     * Larger hierarchy structure:
     * <pre>
     * {@code
     *                          21 (root)
     *                           │
     *                          20
     *                ┌──────────┴──────────┐
     *               0                      15
     *          ┌─────┐
     *         ┌┘     └┐
     *        1         2
     *     ┌──┴──┐   ┌──┴──┐
     *    3      4   5      6
     *  ┌─┴─┐ ┌─┴─┐ ┌─┴─┐ ┌─┴─┐
     *  7   8 9  10 11 12 13 14
     * }
     * </pre>
     *
     * After modification (moving node 1 to be child of 15):
     * <pre>
     * {@code
     *                          21 (root)
     *                           │
     *                          20
     *                ┌──────────┴──────────┐
     *               0                      15
     *               │                       │
     *               2                       1
     *            ┌──┴──┐                 ┌──┴──┐
     *           5      6                3      4
     *         ┌─┴─┐  ┌─┴─┐            ┌─┴─┐  ┌─┴─┐
     *        11 12  13 14             7   8  9  10
     * }
     * </pre>
     */
    @Test
    void descendantsAcrossContinuationsAndChangingHierarchy() throws Exception {
        try (var ddl = Ddl.builder()
                .database(URI.create("/TEST/RECURSIVE_LARGE"))
                .relationalExtension(relationalExtension)
                .schemaTemplate(HIERARCHY_SCHEMA)
                .build()) {

            try (RelationalStatement statement = ddl.setSchemaAndGetConnection().createStatement()) {
                // Insert the larger hierarchy
                insertLargerHierarchy(statement);

                // First query: Get descendants of node 21 (root) with PREORDER traversal, limit to 5 rows
                String descendantsQuery =
                        "WITH RECURSIVE descendants(id, parent, etag) AS (" +
                                "  SELECT id, parent, etag FROM HierarchyNode WHERE id = 21 AND parent = -1 " +
                                "  UNION ALL " +
                                "  SELECT h.id, h.parent, h.etag " +
                                "  FROM HierarchyNode h, descendants d where h.parent = d.id " +
                                ") " +
                                "traversal order pre_order " +
                                "with check etag on mismatch continue " +
                                "SELECT id, parent, etag FROM descendants";

                statement.setMaxRows(5);
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

                // First batch should contain: 21, 20, 0, 1, 3 (PREORDER traversal), all with etag=0
                Assertions.assertEquals(List.of(21L, 20L, 0L, 1L, 3L), firstBatch,
                        "First batch should contain nodes 21, 20, 0, 1, 3 in PREORDER");
                Assertions.assertNotNull(continuation, "Should have a continuation");
                System.out.println("First batch with etag: " + firstBatchWithEtag);

                /*
                 * Modify the hierarchy: Move node 1 from parent 0 to parent 15 and set etag to 1
                 * Also update etag on nodes 21 and 20 to demonstrate etag tracking across multiple levels
                 */
                int updateCount = statement.executeUpdate("UPDATE HierarchyNode SET parent = 15, etag = 1 WHERE id = 1");
                Assertions.assertEquals(1, updateCount, "Should update exactly one row");
                updateCount = statement.executeUpdate("UPDATE HierarchyNode SET etag = 1 WHERE id = 20");
                Assertions.assertEquals(1, updateCount, "Should update exactly one row");
//                updateCount = statement.executeUpdate("UPDATE HierarchyNode SET etag = 1 WHERE id = 20");
//                Assertions.assertEquals(1, updateCount, "Should update exactly one row");

                // Print all nodes to see the current state
                System.out.println("\nHierarchy state after updates:");
                try (var ps = ddl.setSchemaAndGetConnection().prepareStatement("SELECT * FROM HierarchyNode ORDER BY id")) {
                    try (RelationalResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            System.out.println("id = " + rs.getLong("id") +
                                             ", parent = " + rs.getLong("parent") +
                                             ", etag = " + rs.getLong("etag"));
                        }
                    }
                }

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
                Assertions.assertFalse(secondBatch.isEmpty(),
                        "Second batch should contain remaining nodes");

                // Verify all nodes were retrieved
                List<Long> allNodes = new ArrayList<>(firstBatch);
                allNodes.addAll(secondBatch);

                System.out.println("\nFirst batch: " + firstBatch);
                System.out.println("Second batch: " + secondBatch);
                System.out.println("Second batch with etag: " + secondBatchWithEtag);
                System.out.println("All nodes: " + allNodes);
                System.out.println("Total nodes retrieved: " + allNodes.size());
            }
        }
    }

    /**
     * Helper method to insert the larger hierarchy using SQL INSERT statements.
     *
     * Hierarchy structure:
     * - Node 21 (root) with parent -1
     * - Node 20 (child of 21) with parent 21
     * - Node 0 (child of 20) with parent 20
     * - Node 15 (child of 20) with parent 20
     * - Then the same subtree structure under node 0 as in the original test
     * All nodes initially have etag = 0
     */
    private void insertLargerHierarchy(RelationalStatement statement) throws SQLException {
        String insertQuery =
                "INSERT INTO HierarchyNode (id, parent, etag) VALUES " +
                        // Top level
                        "(21, -1, 0), " +  // Root node 21
                        "(20, 21, 0), " +  // Node 20, child of 21
                        // Second level - both 0 and 15 are children of 20
                        "(0, 20, 0), " +   // Node 0, child of 20
                        "(15, 20, 0), " +  // Node 15, child of 20
                        // Original hierarchy under node 0
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
        Assertions.assertEquals(18, count, "Should insert exactly 18 nodes");

        System.out.println("Inserted 18 nodes into the larger hierarchy");
    }
}
