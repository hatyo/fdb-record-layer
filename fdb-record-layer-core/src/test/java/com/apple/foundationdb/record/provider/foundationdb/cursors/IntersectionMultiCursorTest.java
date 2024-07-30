/*
 * IntersectionMultiCursorTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for intersecting aggregates.
 */
public class IntersectionMultiCursorTest extends FDBRecordStoreQueryTestBase {

    protected void setupHookAndAddData(@Nonnull FDBRecordContext context) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = (metaDataBuilder) -> {
            complexQuerySetupHook().apply(metaDataBuilder);
            metaDataBuilder.addIndex("MySimpleRecord", "MySimpleRecord$num_value_2", field("num_value_2"));
            metaDataBuilder.addIndex("MySimpleRecord", new Index("SumIndex", field("num_value_3_indexed").groupBy(field("num_value_2")), IndexTypes.SUM));
            metaDataBuilder.addIndex("MySimpleRecord", new Index("CountIndex", field("num_value_3_indexed").groupBy(field("num_value_2")), IndexTypes.COUNT_NOT_NULL));
        };
        openSimpleRecordStore(context, hook);
        var rec = TestRecords1Proto.MySimpleRecord.newBuilder();
        rec.setRecNo(1).setStrValueIndexed("1").setNumValueUnique(1).setNumValue2(1).setNumValue3Indexed(10);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(2).setStrValueIndexed("2").setNumValueUnique(2).setNumValue2(1).setNumValue3Indexed(20);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(3).setStrValueIndexed("3").setNumValueUnique(3).setNumValue2(1).setNumValue3Indexed(30);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(4).setStrValueIndexed("4").setNumValueUnique(4).setNumValue2(2).setNumValue3Indexed(5);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(5).setStrValueIndexed("5").setNumValueUnique(5).setNumValue2(2).setNumValue3Indexed(5);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(6).setStrValueIndexed("6").setNumValueUnique(6).setNumValue2(2).setNumValue3Indexed(5);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(7).setStrValueIndexed("7").setNumValueUnique(7).setNumValue2(3).setNumValue3Indexed(-10);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(8).setStrValueIndexed("8").setNumValueUnique(8).setNumValue2(3).setNumValue3Indexed(-20);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(9).setStrValueIndexed("9").setNumValueUnique(9).setNumValue2(3).setNumValue3Indexed(-30);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(10).setStrValueIndexed("10").setNumValueUnique(10).setNumValue2(4).setNumValue3Indexed(100);
        recordStore.saveRecord(rec.build());
        rec.setRecNo(11).setStrValueIndexed("11").setNumValueUnique(11).setNumValue2(4).setNumValue3Indexed(2000);
        recordStore.saveRecord(rec.build());
    }

    @Test
    public void intersectionMultiCursorTest() throws Exception {
        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN;
        try (FDBRecordContext context = openContext()) {
            setupHookAndAddData(context);
            final Index sumIndex = recordStore.getRecordMetaData().getIndex("SumIndex");
            final Index countIndex = recordStore.getRecordMetaData().getIndex("CountIndex");

            List<String> recNos = IntersectionMultiCursor.create(
                            (IndexEntry entry) -> TupleHelpers.subTuple(entry.getKey(), 1, entry.getKey().size()).getItems(),
                            false,
                            ImmutableList.of(
                                    (byte[] leftContinuation) -> recordStore.scanIndex(sumIndex, IndexScanType.BY_GROUP, TupleRange.ALL, leftContinuation, scanProperties),
                                    (byte[] rightContinuation) -> recordStore.scanIndex(countIndex, IndexScanType.BY_GROUP, TupleRange.ALL, rightContinuation, scanProperties)),
                            null,
                            recordStore.getTimer())
                    .mapPipelined(indexEntryList -> CompletableFuture.supplyAsync(() -> indexEntryList.stream().map(entry -> entry.getKey() + " -> " +
                            entry.getValue()).collect(Collectors.joining(", "))), recordStore.getPipelineSize(PipelineOperation.INDEX_TO_RECORD))
                    .asList()
                    .get();
            assertEquals(ImmutableList.of("(1) -> (60), (1) -> (3)", "(2) -> (15), (2) -> (3)", "(3) -> (-60), (3) -> (3)", "(4) -> (2100), (4) -> (2)"), recNos);
            commit(context);
        }
    }
}
