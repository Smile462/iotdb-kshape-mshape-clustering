/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class PageKCorrectionTest {
  private void payloadEquals(Statistics expected, Statistics actual) {
    assertArrayEquals(expected.idx, actual.idx);
    assertArrayEquals(expected.deltas, actual.deltas, 0);
    for (int c = 0; c < expected.centroids.length; c++) {
      assertArrayEquals(expected.centroids[c], actual.centroids[c], 0);
      for (int j = 0; j < expected.centroids[c].length; j++)
        assertArrayEquals(expected.sumMatrices[c][j], actual.sumMatrices[c][j], 0);
    }
  }

  @Test
  public void chunkKUsesOriginalPageSubtractionWithoutTemporaryFit() throws Exception {
    int originalK = TSFileDescriptor.getInstance().getConfig().getClusterNum();
    int originalL = TSFileDescriptor.getInstance().getConfig().getSeqLength();
    PrintStream original = System.out;
    try {
      TSFileDescriptor.getInstance().getConfig().setClusterNum(3);
      TSFileDescriptor.getInstance().getConfig().setSeqLength(16);
      AggregationPlan plan = new AggregationPlan();
      plan.setParameters(Collections.singletonList(Collections.emptyMap()));
      KshapeExecutor executor = new KshapeExecutor(null, plan);
      for (String name : Arrays.asList("k", "l")) {
        Field field = KshapeExecutor.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setInt(executor, name.equals("k") ? 3 : 16);
      }
      Method correction =
          KshapeExecutor.class.getDeclaredMethod(
              "updateByIter",
              IBatchDataIterator.class,
              Statistics.class,
              long.class,
              long.class,
              long.class);
      correction.setAccessible(true);
      Statistics stored = new DoubleStatistics();
      for (int time = 0; time < 640; time++) stored.update(time, PageMCorrectionTest.value(time));
      stored.updateStatistics();
      Statistics untouched = stored.clone();
      ByteArrayOutputStream log = new ByteArrayOutputStream();
      System.setOut(new PrintStream(log, true, "UTF-8"));
      for (int[] query :
          new int[][] {{16, 639}, {0, 623}, {17, 622}, {48, 127}, {48, 63}, {1, 15}}) {
        int start = query[0], end = query[1];
        Statistics expected =
            (Statistics)
                correction.invoke(
                    executor,
                    PageMCorrectionTest.page(0, 639).getBatchDataIterator(),
                    stored.clone(),
                    (long) start,
                    (long) end,
                    -1L);
        JournalSelection selection =
            new JournalSelection(Collections.emptyMap(), start, end, 16, 3);
        List<Statistics> units = new ArrayList<>();
        BatchData raw = PageMCorrectionTest.page(0, 639);
        selection.addCorrectedPage(
            units,
            raw,
            stored,
            (batch, copy) -> {
              try {
                return (Statistics)
                    correction.invoke(
                        executor,
                        batch.getBatchDataIterator(),
                        copy,
                        (long) start,
                        (long) end,
                        -1L);
              } catch (ReflectiveOperationException error) {
                throw new RuntimeException(error);
              }
            });
        assertEquals(1, units.size());
        assertEquals(1, selection.correctedPages);
        assertEquals(0, selection.temp);
        Statistics actual = units.get(0);
        payloadEquals(expected, actual);
        assertEquals(start, actual.getStartTime());
        assertEquals(end, actual.getEndTime());
        assertTrue(actual.journalGridFragments);
        payloadEquals(untouched, stored);
        assertArrayEquals(untouched.headExtraPoints, stored.headExtraPoints, 0);
        assertArrayEquals(untouched.tailExtraPoints, stored.tailExtraPoints, 0);
      }
      assertFalse(log.toString("UTF-8").contains("JOURNAL_BUILD_PAGE_NS="));
    } finally {
      System.setOut(original);
      TSFileDescriptor.getInstance().getConfig().setClusterNum(originalK);
      TSFileDescriptor.getInstance().getConfig().setSeqLength(originalL);
    }
  }
}
